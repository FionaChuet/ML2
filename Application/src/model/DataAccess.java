package model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Provides a generic booking application with high-level methods to access its
 * data store, where the booking information is persisted. The class also hides
 * to the application the implementation of the data store (whether a Relational
 * DBMS, XML files, Excel sheets, etc.) and the complex machinery required to
 * access it (SQL statements, XQuery calls, specific API, etc.).
 * <p>
 * The booking information includes:
 * <ul>
 * <li> A collection of seats (whether theater seats, plane seats, etc.)
 * available for booking. Each seat is uniquely identified by a number in the
 * range {@code [1, seatCount]}. For the sake of simplicity, there is no notion
 * of row and column, as in a theater or a plane: seats are considered
 * <i>adjoining</i> if they bear consecutive numbers.
 * <li> A price list including various categories. For the sake of simplicity,
 * (a) the price of a seat only depends on the customer, e.g. adult, child,
 * retired, etc., not on the location of the seat as in a theater, (b) each
 * price category is uniquely identified by an integer in the range
 * {@code [0, categoryCount)}, e.g. {@code 0}=adult, {@code 1}=child,
 * {@code 2}=retired. (Strings or symbolic constants, like Java {@code enum}s,
 * are not used.)
 * </ul>
 * <p>
 * A typical booking scenario involves the following steps:
 * <ol>
 * <li> The customer books one or more seats, specifying the number of seats
 * requested in each price category with
 * {@link #bookSeats(String, List, boolean)}. He/She lets the system select the
 * seats to book from the currently-available seats.
 * <li> Alternatively, the customer can check the currently-available seats with
 * {@link #getAvailableSeats(boolean)} and then specify the number of the seats
 * he/she wants to book in each price category with
 * {@link #bookSeats(String, List)}.
 * <li> Later on, the customer can change his/her mind and cancel with
 * {@link #cancelBookings(List)} one or more of the bookings he/she previously
 * made.
 * <li> At any time, the customer can check the bookings he/she currently has
 * with {@link #getBookings(String)}.
 * <li> The customer can repeat the above steps any number of times.
 * </ol>
 * <p>
 * The constructor and the methods of this class all throw a
 * {@link DataAccessException} when an unrecoverable error occurs, e.g. the
 * connection to the data store is lost. The methods of this class never return
 * {@code null}. If the return type of a method is {@code List<Item>} and there
 * is no item to return, the method returns the empty list rather than
 * {@code null}.
 * <p>
 * <i>Notes to implementors: </i>
 * <ol>
 * <li> <b>Do not</b> modify the interface of the classes in the {@code model}
 * package. If you do so, the test programs will not compile. Also, remember
 * that the exceptions that a method throws are part of the method's interface.
 * <li> The test programs will abort whenever a {@code DataAccessException} is
 * thrown: make sure your code throws a {@code DataAccessException} only when a
 * severe (i.e. unrecoverable) error occurs.
 * <li> {@code JDBC} often reports constraint violations by throwing an
 * {@code SQLException}: if a constraint violation is intended, make sure your
 * your code does not report it as a {@code DataAccessException}.
 * <li> The implementation of this class must withstand failures (whether
 * client, network of server failures) as well as concurrent accesses to the
 * data store through multiple {@code DataAccess} objects.
 * </ol>
 *
 * @author Jean-Michel Busca
 */
public class DataAccess implements AutoCloseable {

    private Connection connection;
    private PreparedStatement dropSeats; // To drop 'seats' relation if any
    private PreparedStatement dropCategories; // To drop 'categories' relation if any
    private PreparedStatement dropBookings; // To drop 'categories' relation if any
    private PreparedStatement createSeats; // To create 'seats' relation
    private PreparedStatement createCategories; // To create 'categories' relation
    private PreparedStatement createBookings; // To create 'categories' relation
    private PreparedStatement insertSeats; // To create 'categories' relation
    private PreparedStatement insertCategories; // To create 'categories' relation
    private PreparedStatement insertBookings; // To create 'categories' relation
    private PreparedStatement updateSeats; // To create 'categories' relation
    private PreparedStatement getPriceList; // To get the price list
    private PreparedStatement getAvailableSeats; // To get the available seats
    private PreparedStatement getBookings; // To get the all bookings or of a specificed customer
    private PreparedStatement checkBooking; // To get the all bookings or of a specificed customer
    private PreparedStatement cancelBookings; // To get the all bookings or of a specificed customer

    /**
     * Creates a new {@code DataAccess} object that interacts with the specified
     * data store, using the specified login and the specified password. Each
     * object maintains a dedicated connection to the data store until the
     * {@link close} method is called.
     *
     * @param url the URL of the data store to connect to
     * @param login the (application) login to use
     * @param password the password
     *
     * @throws DataAccessException if an unrecoverable error occurs
     */
    public DataAccess(String url, String login, String password) throws
            DataAccessException {
        try {
            // Connect to the database
            connection = DriverManager.getConnection(url, login, password);
            connection.setAutoCommit(false);
        } catch (SQLException ex) {
            try {
                try {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                    connection.rollback();
                    connection.setAutoCommit(true);
                } catch (SQLException ex1) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
                }
                connection.commit();
                connection.setAutoCommit(true);
            } catch (SQLException ex1) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }
    }

    /**
     * Initializes the data store according to the specified number of seats and
     * the specified price list. If the data store is already initialized or if
     * it already contains bookings when this method is called, it is reset and
     * initialized again from scratch. When the method completes, the state of
     * the data store is as follows: all the seats are available for booking, no
     * booking is made.
     * <p>
     * <i>Note to implementors: </i>
     * <ol>
     * <li>All the information provided by the parameters must be persisted in
     * the data store. (It must not be kept in Java instance or class
     * attributes.) To enforce this, the data store will be initialized by
     * running the {@code DataStoreInit} program, and then tested by running the
     * {@code SingleUserTest} and {@code MultiUserTest} programs.
     * <li><b>Do not use</b> the statements {@code drop database ...} and
     * {@code create database ...} in this method, as the test program might not
     * have sufficient privileges to execute them. Use the statements {@code drop table
     * ...} and {@code create table ...} instead.
     * <li>The schema of the database (in the sense of "set of tables") is left
     * unspecified. You can select any schema that fulfills the requirements of
     * the {@code DataAccess} methods.
     * </ol>
     *
     * @param seatCount the total number of seats available for booking
     * @param priceList the price for each price category
     *
     * @return {@code true} if the data store was initialized successfully and
     * {@code false} otherwise
     *
     * @throws DataAccessException if an unrecoverable error occurs
     */
    public boolean initDataStore(int seatCount, List<Float> priceList) throws DataAccessException {
        try {
            connection.setAutoCommit(false);
            /* We need to create 3 relations:
        - First on is the 'seats' relation, which holds all the seats information about seats.
        (number, booking as names of customers, category (id))
        - Second is the 'categories' relation, which holds all information about categories price.
        - Third is the 'bookings' relation.
        
        To create these relations, we use a lazy initialisation/design pattern with prepared statements.
             */

            // 0. Dropping relations in case they already exist in database
            // Creating prepared statements
            dropSeats = connection.prepareStatement("drop table seats");
            dropCategories = connection.prepareStatement("drop table categories");
            dropBookings = connection.prepareStatement("drop table bookings");

            // Dropping tables if exist
            try {
                dropSeats.executeUpdate();
            } catch (SQLException ex) {
                System.err.print("Table 'seats' does not exist! Creating it...\n");
            }
            try {
                dropCategories.executeUpdate();
            } catch (SQLException ex) {
                System.err.print("Table 'categories' does not exist! Creating it...\n");
            }
            try {
                dropBookings.executeUpdate();
            } catch (SQLException ex) {
                System.err.print("Table 'bookings' does not exist! Creating it...\n");
            }
            
            // 1. Creating the 'categories' relations
            // Create the relation
            createCategories = connection.prepareStatement("create table categories (id integer not null,"
                    + "name varchar(20) not null unique,"
                    + "price float not null,"
                    + "primary key (id))");
            createCategories.executeUpdate();
            // Insert into table
            insertCategories = connection.prepareStatement("insert into categories (id, name, price)"
                    + "values (0, 'adult', ?),"
                    + "(1, 'child', ?),"
                    + "(2, 'retired', ?)");
            if (priceList.size() == 1) {
                for (int j = 0; j < 3; j++) {
                    insertCategories.setFloat((j + 1), priceList.get(0));
                }
            } else {
                for (int i = 0; i < priceList.size(); i++) {
                    insertCategories.setFloat((i + 1), priceList.get(i));
                }
            }
            insertCategories.executeUpdate();

            // 2. Creating the 'seats' relations
            createSeats = connection.prepareStatement("create table seats (id integer not null auto_increment,"
                    + "available boolean default true,"
                    + "primary key (id))");
            createSeats.executeUpdate();
            // Insert into table
            insertSeats = connection.prepareStatement("insert into seats (id, available) values (null, true)");
            for (int i = 0; i < seatCount; i++) {
                insertSeats.addBatch();
            }
            insertSeats.executeBatch();

            // 3. Creating the 'bookings' relations
            createBookings = connection.prepareStatement("create table bookings (id integer not null auto_increment,"
                    + "seat integer unique null default null,"
                    + "customer varchar(30) null default null,"
                    + "category integer null default null,"
                    + "primary key (id),"
                    + "foreign key (seat) references seats(id),"
                    + "foreign key (category) references categories(id))");
            createBookings.executeUpdate();
            return true;

        } catch (SQLException ex) {
            try {
                try {
                    System.err.println("Error when creating tables (SQL): " + ex);
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                    connection.rollback();
                    connection.setAutoCommit(true);
                } catch (SQLException ex1) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
                }
                connection.commit();
                connection.setAutoCommit(true);
            } catch (SQLException ex1) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }
        return false;

    }

    /**
     * Returns the price list.
     * <p>
     * <i>Note to implementors: </i>  <br> The method must return the price list
     * persisted in the data store.
     *
     * @return the price list
     *
     * @throws DataAccessException if an unrecoverable error occurs
     */
    public List<Float> getPriceList() throws DataAccessException {
        try {
            // create the prepared statement, if not created yet
            connection.setAutoCommit(false);
            try {
                if (getPriceList == null) {
                    
                    getPriceList = connection.prepareStatement("SELECT price FROM categories");
                }
            } catch (SQLException ex) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
            }
            // execute the prepared statement; whatever happens, the try-with-resource construct will close the result set
            try (ResultSet result = getPriceList.executeQuery()) {
                List<Float> list = new ArrayList<>();
                while (result.next()) {
                    list.add(result.getFloat(1));
                }
                return list;
            } catch (SQLException ex) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            try {
                try {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                    connection.rollback();
                    connection.setAutoCommit(true);
                    throw ex;
                } catch (SQLException ex1) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
                }
                connection.commit();
                connection.setAutoCommit(true);
            } catch (SQLException ex1) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }
    return Collections.EMPTY_LIST;
    }

    /**
     * Returns the available seats in the specified mode. Two modes are
     * provided:
     * <i>stable</i> or not. If the stable mode is selected, the returned seats
     * are guaranteed to remain available until one of the {@code bookSeats}
     * methods is called on this data access object. If the stable mode is not
     * selected, the returned seats might have been booked by another user when
     * one of these methods is called. Regardless of the mode, the available
     * seats are returned in ascending order of number.
     * <p>
     * <i>Note to implementors: </i> <br> The stable mode is defined as an
     * exercise. It cannot be used in a production application as this would
     * prevent all other users from retrieving the available seats until the
     * current user decides which seats to book.
     *
     * @param stable {@code true} to select the stable mode and {@code false}
     * otherwise
     *
     * @return the available seats in ascending order or the empty list if no
     * seat is available
     *
     * @throws DataAccessException if an unrecoverable error occurs
     */
    public List<Integer> getAvailableSeats(boolean stable) throws DataAccessException {
        try {
            // create the prepared statement, if not created yet
            connection.setAutoCommit(false);
            if (getAvailableSeats == null) {
                try {
                    getAvailableSeats = connection.prepareStatement("SELECT id FROM seats WHERE available = 1 order by id");
                } catch (SQLException ex) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            // execute the prepared statement; whatever happens, the try-with-resource construct will close the result set
            try (ResultSet result = getAvailableSeats.executeQuery()) {
                List<Integer> list = new ArrayList<>();
                while (result.next()) {
                    list.add(result.getInt(1));
                }
                return list;
            } catch (SQLException ex) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            try {
                try {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                    connection.rollback();
                    connection.setAutoCommit(true);
                    throw ex;
                } catch (SQLException ex1) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
                }
                connection.commit();
                connection.setAutoCommit(true);
            } catch (SQLException ex1) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Books the specified number of seats for the specified customer in the
     * specified mode. The number of seats to book is specified for each price
     * category. Two modes are provided: <i>adjoining</i> or not. If the
     * adjoining mode is selected, the method guarantees that the booked seats
     * are adjoining. If the adjoining mode is not selected, the returned seats
     * might be apart. Regardless of the mode, the bookings are returned in
     * ascending order of seat number.
     * <p>
     * If the specified customer already has bookings, the adjoining mode only
     * applies to the new bookings: The method will not try to select seats
     * adjoining to already-booked seats by the same customer.
     * <p>
     * The method executes in an all-or-nothing fashion: if there are not enough
     * available seats left or if the seats are not adjoining while the
     * adjoining mode is selected, then no seat is booked.
     *
     * @param customer the customer who makes the booking
     * @param counts the count of seats to book for each price category:
     * counts.get(i) is the count of seats to book in category #i
     * @param adjoining {@code true} to select the adjoining mode and
     * {@code false} otherwise
     *
     * @return the list of bookings if the booking was successful or the empty
     * list otherwise
     *
     * @throws DataAccessException if an unrecoverable error occurs
     */
    public List<Booking> bookSeats(String customer, List<Integer> counts, boolean adjoining) throws DataAccessException {
        try {
            connection.setAutoCommit(false);
            // Get the specified seats to book in each category (0: adult, 1: child, 2: retired)
            boolean number = true; // Check whether or not the number of seats to book are not greater than the number of available seats
            boolean customerName = true; // Check whether or no the customer entered is right (not null for instance)
            int totalSeats = 0;  // Total number of seats to book
            List<Integer> seatsToBook = new ArrayList();
            
            // Get the number of seats to book
            for(int m = 0; m < counts.size(); m++){
                totalSeats += counts.get(m);
            }

            // Get list of available seats
            List<Integer> availableSeats = getAvailableSeats(true);

            // Check whether or not the values inside the available seats are continguous or not
            // availableSeats is always sorted by ID (query)
            if (adjoining) {
                for (int k = 0; k < availableSeats.size() - 1; k++) {
                    // Continguous seats                   
                    if(availableSeats.get(k) == (availableSeats.get(k + 1) - 1)){
                        if(!seatsToBook.contains(k)){
                            seatsToBook.add(k);
                        }
                        if(!seatsToBook.contains(k+1)){
                            seatsToBook.add(k+1);
                        }    
                    }
                }
            }
            else {
                seatsToBook = availableSeats;
            }

            // If the number of seats to book is greater than the number of available (continguous or not) seats
            if (totalSeats > seatsToBook.size()) {
                number = false;
                System.err.print("There are not enough available seats to book ! Cancel the booking...\n");
            }
            
            // Check is customer names if right
            if((customer.equals(null)) || (customer.equals(""))){
                customerName = false;
                System.err.print("The customer's name is not right ! Cancel the booking...\n");
            }

            // None of the seats to book are already used: we can book them
            if (number && customerName) {
                //get the list of available seats
                List<Float> priceList = getPriceList();
                List<Booking> list = new ArrayList<>();
                int index = 0; // Index of available seats

                //Prepared statement for bookings
                insertBookings = connection.prepareStatement("insert into bookings (id, seat, customer, category) values (null, ?, ?, ?)");
                if (updateSeats == null) {
                    updateSeats = connection.prepareStatement("update seats set available = ? where id = ?");
                } else {
                    updateSeats.clearParameters();
                }

                // Check if all the seats to book are available; if not, cancel the operation
                for (int j = 0; j < counts.size(); j++) {
                    for (int l = 0; l < counts.get(j); l++) {
                        insertBookings.setInt(1, seatsToBook.get(index));
                        insertBookings.setString(2, customer);
                        insertBookings.setInt(3, j);
                        insertBookings.addBatch();
                        updateSeats.setInt(1, 0);
                        updateSeats.setInt(2, seatsToBook.get(index));
                        updateSeats.addBatch();
                        list.add(new Booking(seatsToBook.get(index), customer, j, priceList.get(j)));
                        index++;
                    }
                }

                // Inserting all bookings into database & updating seats table for booked seats
                insertBookings.executeBatch();
                updateSeats.executeBatch();

                //Do not need to sort the list returned as the index inserted are already sorted by ID thanks to the getAvailableSeats method
                return list;
            }
        } catch (SQLException ex) {
            try {
                try {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                    connection.rollback();
                    connection.setAutoCommit(true);
                    throw ex;
                } catch (SQLException ex1) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
                }
                connection.commit();
                connection.setAutoCommit(true);
            } catch (SQLException ex1) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Books the specified seats for the specified customer. The seats to book
     * are specified for each price category.
     * <p>
     * The method executes in an all-or-nothing fashion: if one of the specified
     * seats cannot be booked because it is not available, then none of them is
     * booked.
     *
     * @param customer the customer who makes the booking
     * @param seatss the list of seats to book in each price category:
     * seatss.get(i) is the list of seats to book in category #i
     *
     * @return the list of the bookings made by this method call or the empty
     * list if no booking was made
     *
     * @throws DataAccessException if an unrecoverable error occurs
     */
    public List<Booking> bookSeats(String customer, List<List<Integer>> seatss)
            throws DataAccessException {
        try {
            connection.setAutoCommit(false);
            // Get the specified seats to book in each category (0: adult, 1: child, 2: retired)
            Map<Integer, List<Integer>> seatsToBook = new HashMap<>();
            boolean notExist = true;
            boolean customerName = true; // Check whether or no the customer entered is right (not null for instance)
            boolean number = true;
            int totalSeats = 0;

            // Get list of bookings
            List<Booking> bookings = getBookings("");
            // Get list of available seats
            List<Integer> availableSeats = getAvailableSeats(true);

            // Check if all the seats to book are available; if not, cancel the operation
            for (int i = 0; i < seatss.size(); i++) {
                totalSeats += seatss.get(i).size();
                for (int j = 0; j < seatss.get(i).size(); j++) {
                    for (Booking a : bookings) {
                        if (a.getSeat() == seatss.get(i).get(j)) {
                            notExist = false;
                            System.err.print("Seat number " + a.getSeat() + " is already booked ! Cancel the booking...\n");
                        }
                    }
                }
            }

            // Check of the number of seats to book are greater than the number of available seats
            if (totalSeats > availableSeats.size()) {
                System.err.print("There are not enough available seats to book ! Cancel the booking...\n");
                number = false;
            }
            
            // Check is customer names if right
            if((customer.equals(null)) || (customer.equals(""))){
                customerName = false;
                System.err.print("The customer's name is not right ! Cancel the booking...\n");
            }

            // None of the seats to book are already used: we can book them
            if (notExist && number && customerName) {
                //get the list of available seats
                List<Float> priceList = getPriceList();
                List<Booking> list = new ArrayList<>();

                //Prepared statement for bookings
                insertBookings = connection.prepareStatement("insert into bookings (id, seat, customer, category) values (null, ?, ?, ?)");
                if (updateSeats == null) {
                    updateSeats = connection.prepareStatement("update seats set available = ? where id = ?");
                } else {
                    updateSeats.clearParameters();
                }

                //New iterator 
                for (int k = 0; k < seatss.size(); k++) {
                    for (int n = 0; n < seatss.get(k).size(); n++) {
                        insertBookings.setInt(1, seatss.get(k).get(n));
                        insertBookings.setString(2, customer);
                        insertBookings.setInt(3, k);
                        insertBookings.addBatch();
                        updateSeats.setInt(1, 0);
                        updateSeats.setInt(2, seatss.get(k).get(n));
                        updateSeats.addBatch();
                        list.add(new Booking(seatss.get(k).get(n), customer, k, priceList.get(k)));
                    }
                }

                // Inserting all bookings into database & updating seats table for booked seats
                insertBookings.executeBatch();
                updateSeats.executeBatch();
                return list;
            }

        } catch (SQLException ex) {
            try {
                try {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                    connection.rollback();
                    connection.setAutoCommit(true);
                    throw ex;
                } catch (SQLException ex1) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
                }
                connection.commit();
                connection.setAutoCommit(true);
            } catch (SQLException ex1) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Returns the current bookings of the specified customer. If no customer is
     * specified, the method returns the current bookings of all the customers.
     *
     * @param customer the customer whose bookings must be returned or the empty
     * string {@code ""} if all the bookings must be returned
     *
     * @return the list of the bookings of the specified customer or the empty
     * list if the specified customer does not have any booking
     *
     * @throws DataAccessException if an unrecoverable error occurs
     */
    public List<Booking> getBookings(String customer) throws DataAccessException {
        try {
            connection.setAutoCommit(false);
            if (getBookings == null) {
                try {
                    if (customer.equals("")) {
                        getBookings = connection.prepareStatement("SELECT bookings.id, bookings.seat, bookings.customer, bookings.category, categories.price FROM bookings INNER JOIN categories ON bookings.category = categories.id");
                    }
                    else {
                        getBookings = connection.prepareStatement("SELECT bookings.id, bookings.seat, bookings.customer, bookings.category, categories.price FROM bookings INNER JOIN categories ON bookings.category = categories.id WHERE bookings.customer = '" + customer + "'");
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            // execute the prepared statement; whatever happens, the try-with-resource construct will close the result set
            try (ResultSet result = getBookings.executeQuery()) {
                List<Booking> list = new ArrayList<>();
                while (result.next()) {
                    
                    // booking(int id, int seat, String customer, int category, float price)
                    list.add(new Booking(result.getInt(1), result.getInt(2), result.getString(3), result.getInt(4), result.getFloat(5)));
                }
                return list;
            } catch (SQLException ex) {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            try {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                connection.rollback();
                connection.setAutoCommit(true);
                throw ex;
            } catch (SQLException ex1) {
                try {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
                    connection.commit();
                    connection.setAutoCommit(true);
                } catch (SQLException ex2) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex2);
                }
            }
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Cancel the specified bookings. The method checks against the data store
     * that each of the specified bookings is valid, i.e. it is assigned to the
     * specified customer, for the specified price category.
     * <p>
     * The method executes in an all-or-nothing fashion: if one of the specified
     * bookings cannot be canceled because it is not valid, then none of them is
     * canceled.
     *
     * @param bookings the bookings to cancel
     *
     * @return {@code true} if all the bookings were canceled and {@code false}
     * otherwise
     *
     * @throws DataAccessException if an unrecoverable error occurs
     */
    public boolean cancelBookings(List<Booking> bookings) throws DataAccessException {
        boolean valid = true; //Boolean that checks whether or not a booking is invalid
        try {
            connection.setAutoCommit(false);
            //Variables
            checkBooking = connection.prepareStatement("select customer, category from bookings where seat = ?"); //seat is unique inside the database
            cancelBookings = connection.prepareStatement("delete from bookings where seat = ?");
            if (updateSeats == null) {
                updateSeats = connection.prepareStatement("update seats set available = ? where id = ?");
            } else {
                updateSeats.clearParameters();
            }

            // Need to check whether or not each booking is valid
            for (Booking a : bookings) {
                checkBooking.setInt(1, a.getSeat()); // For the specified seat
                try (ResultSet result = checkBooking.executeQuery()) {
                    result.next();
                    if ((!result.getString(1).equals(a.getCustomer())) || (result.getInt(2) != a.getCategory())) // Get both customer and price category
                    {
                        valid = false; // If any of the booking is invalid
                        System.err.print("Booking for seat number " + a.getSeat() + " is not valid ! Cancel operation...\n");
                        break;
                    }
                }
                checkBooking.clearParameters(); // Clear the parameter to chek another booking with a different seat number
            }

            // None of the bookings to cancel are invalid
            if (valid) {
                for (int i = 0; i < bookings.size(); i++) {
                    cancelBookings.setInt(1, bookings.get(i).getSeat());
                    cancelBookings.addBatch();
                    updateSeats.setInt(1, 1);
                    updateSeats.setInt(2, bookings.get(i).getSeat());
                    updateSeats.addBatch();
                }

                // Execute queries
                cancelBookings.executeBatch();
                updateSeats.executeBatch();
            }

        } catch (SQLException ex) {
            try {
                Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                connection.rollback();
                connection.setAutoCommit(true);
                throw ex;
            } catch (SQLException ex1) {
                try {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
                    connection.commit();
                    connection.setAutoCommit(true);
                } catch (SQLException ex2) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex2);
                }
            }
        }
        return valid;
    }

    /**
     * Closes this data access object. This closes the underlying connection to
     * the data store and releases all related resources. The application must
     * call this method when it is done using this data access object.
     *
     * @throws DataAccessException if an unrecoverable error occurs
     */
    @Override
    public void close() throws DataAccessException {
            try {
                connection.setAutoCommit(false);
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (SQLException ex) {
                try {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
                    connection.rollback();
                    connection.setAutoCommit(true);
                    throw ex;
                } catch (SQLException ex1) {
                    Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex1);
                }
            }
    }    
}
