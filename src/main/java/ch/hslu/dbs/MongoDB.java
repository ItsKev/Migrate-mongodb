package ch.hslu.dbs;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.sql.*;

public class MongoDB {

    public static void main(String[] args) {

        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase uniDatabase = mongoClient.getDatabase("uni");
        MongoCollection<Document> studenten = uniDatabase.getCollection("studenten");
        MongoCollection<Document> professoren = uniDatabase.getCollection("professoren");
        MongoCollection<Document> assistenten = uniDatabase.getCollection("assistenten");
        MongoCollection<Document> vorlesungen = uniDatabase.getCollection("vorlesungen");
        MongoCollection<Document> pruefung = uniDatabase.getCollection("pruefung");

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            Connection connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/uni?"
                            + "user=root&password=1234&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");

            Statement statement = connect.createStatement();
            migrateProfessors(professoren, statement);
            migrateAssistents(professoren, assistenten, statement);
            migrateLectures(professoren, vorlesungen, statement);
            migrateStudents(studenten, vorlesungen, statement);
            migrateExams(studenten, professoren, vorlesungen, pruefung, statement);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void migrateExams(MongoCollection<Document> studenten, MongoCollection<Document> professoren, MongoCollection<Document> vorlesungen, MongoCollection<Document> pruefung, Statement statement) throws SQLException {
        ResultSet resultSet;
        resultSet = statement
                .executeQuery("SELECT * FROM pruefen");
        while (resultSet.next()) {
            Document document = new Document("PersNr", professoren.find(Filters.eq("PersNr", resultSet.getInt("PersNr"))).first());
            document.append("MatrNr", studenten.find(Filters.eq("MatrNr", resultSet.getInt("MatrNr"))).first());
            document.append("VorlNr", vorlesungen.find(Filters.eq("VorlNr", resultSet.getInt("VorlNr"))).first());
            document.append("Note", resultSet.getFloat("Note"));
            pruefung.insertOne(document);
        }
    }

    private static void migrateStudents(MongoCollection<Document> studenten, MongoCollection<Document> vorlesungen, Statement statement) throws SQLException {
        ResultSet resultSet;
        resultSet = statement
                .executeQuery("SELECT studenten.MatrNr, studenten.Name, studenten.Semester FROM studenten");
        while (resultSet.next()) {
            Document document = new Document("MatrNr", resultSet.getInt("MatrNr"));
            document.append("Name", resultSet.getString("Name"));
            document.append("Semester", resultSet.getInt("Semester"));
            studenten.insertOne(document);
        }

        resultSet = statement
                .executeQuery("SELECT studenten.MatrNr, hoeren.VorlNr FROM studenten INNER JOIN hoeren ON studenten.MatrNr = hoeren.MatrNr");
        while (resultSet.next()) {
            Document vorlesung = vorlesungen.find(Filters.eq("VorlNr", resultSet.getInt("VorlNr"))).first();

            Bson filter = new Document("MatrNr", resultSet.getInt("MatrNr"));
            Bson updateOperationDocument = new Document("$addToSet", new Document("Vorl", vorlesung));
            studenten.updateOne(filter, updateOperationDocument);
        }
    }

    private static void migrateLectures(MongoCollection<Document> professoren, MongoCollection<Document> vorlesungen, Statement statement) throws SQLException {
        ResultSet resultSet;
        resultSet = statement.executeQuery("SELECT vorlesungen.VorlNr, vorlesungen.SWS, vorlesungen.Titel, professoren.PersNr FROM vorlesungen INNER JOIN professoren ON vorlesungen.gelesenVon = professoren.PersNr");
        while (resultSet.next()) {
            Document document = new Document("VorlNr", resultSet.getInt("VorlNr"));
            document.append("Titel", resultSet.getString("Titel"));
            document.append("KP", resultSet.getInt("SWS"));
            Document prof = professoren.find(Filters.eq("PersNr", resultSet.getInt("PersNr"))).first();
            document.append("gelesenVon", prof);
            vorlesungen.insertOne(document);
        }


        resultSet = statement.executeQuery("SELECT vorlesungen.VorlNr, voraussetzen.Nachfolger FROM vorlesungen INNER JOIN voraussetzen ON vorlesungen.VorlNr = voraussetzen.Vorgänger");
        while (resultSet.next()) {
            Document nachfolger = vorlesungen.find(Filters.eq("VorlNr", resultSet.getInt("Nachfolger"))).first();

            Bson filter = new Document("VorlNr", resultSet.getInt("VorlNr"));
            Bson updateOperationDocument = new Document("$addToSet", new Document("Nachfolger", nachfolger));
            vorlesungen.updateOne(filter, updateOperationDocument);
        }

        resultSet = statement.executeQuery("SELECT vorlesungen.VorlNr, voraussetzen.Vorgänger FROM vorlesungen INNER JOIN voraussetzen ON vorlesungen.VorlNr = voraussetzen.Nachfolger");
        while (resultSet.next()) {
            Document vorgaenger = vorlesungen.find(Filters.eq("VorlNr", resultSet.getInt("Vorgänger"))).first();

            Bson filter = new Document("VorlNr", resultSet.getInt("VorlNr"));
            Bson updateOperationDocument = new Document("$addToSet", new Document("Vorgaenger", vorgaenger));
            vorlesungen.updateOne(filter, updateOperationDocument);
        }
    }

    private static void migrateAssistents(MongoCollection<Document> professoren, MongoCollection<Document> assistenten, Statement statement) throws SQLException {
        ResultSet resultSet;
        resultSet = statement.executeQuery("SELECT assistenten.PersNr, assistenten.Name, assistenten.Fachgebiet, professoren.PersNr AS Boss FROM assistenten INNER JOIN professoren ON assistenten.Boss = professoren.PersNr");
        while (resultSet.next()) {
            Document document = new Document("PersNr", resultSet.getInt("PersNr"));
            document.append("Name", resultSet.getString("Name"));
            document.append("Fachgebiet", resultSet.getString("Fachgebiet"));
            Document prof = professoren.find(Filters.eq("PersNr", resultSet.getInt("Boss"))).first();
            document.append("Boss", prof.get("_id"));
            assistenten.insertOne(document);
        }
    }

    private static void migrateProfessors(MongoCollection<Document> professoren, Statement statement) throws SQLException {
        ResultSet resultSet = statement.executeQuery("SELECT * FROM professoren");
        while (resultSet.next()) {
            Document document = new Document("PersNr", resultSet.getInt("PersNr"));
            document.append("Name", resultSet.getString("Name"));
            document.append("Rang", resultSet.getString("Rang"));
            document.append("Raum", resultSet.getString("Raum"));
            professoren.insertOne(document);
        }
    }
}
