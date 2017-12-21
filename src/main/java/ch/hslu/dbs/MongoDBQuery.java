package ch.hslu.dbs;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoDBQuery {
    public static void main(String[] args) {
        // Get db collections
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase uniDatabase = mongoClient.getDatabase("uni");
        MongoCollection<Document> studenten = uniDatabase.getCollection("studenten");
        MongoCollection<Document> professoren = uniDatabase.getCollection("professoren");
        MongoCollection<Document> vorlesungen = uniDatabase.getCollection("vorlesungen");

        // get local, iterable collections
        FindIterable<Document> allProfessoren = professoren.find();
        FindIterable<Document> allStudents = studenten.find();
        FindIterable<Document> allVorlesungen = vorlesungen.find();

        List<Document> query = new ArrayList<>();

        for (Document professor : allProfessoren) {
            professor.append("AnzahlStudenten", 0);
            professor.append("KP", 0);
            query.add(professor);
        }

        getAnzStudenten(allStudents, query);
        getKp(allVorlesungen, query);

        System.out.println("Name | Anzahl Studenten | KP");

        query.sort((o1, o2) -> {
            String firstProfName = o1.getString("Name");
            String secondProfName = o2.getString("Name");

            return firstProfName.compareTo(secondProfName);
        });

        query.forEach(document -> {
            if (document.getInteger("KP") == 0) return;
            String profName = document.getString("Name");
            String anzStudenten = document.getInteger("AnzahlStudenten").toString();
            String kp = document.getInteger("KP").toString();

            System.out.println(profName + " | " + anzStudenten + " | " + kp);
        });
    }

    private static void getAnzStudenten(FindIterable<Document> allStudents, List<Document> query) {
        for (Document d : allStudents) {
            if (d.get("Vorl") == null) continue;
            ArrayList<Document> studentBesuchteVorlesungen = (ArrayList<Document>) d.get("Vorl");
            for (Document besuchteVorlesung : studentBesuchteVorlesungen) {
                query.forEach(document -> {
                    if (document.getObjectId("_id").equals(((Document) besuchteVorlesung.get("gelesenVon")).get("_id"))) {
                        Integer anz = document.getInteger("AnzahlStudenten");
                        anz++;
                        document.replace("AnzahlStudenten", anz);
                    }
                });
            }
        }
    }

    private static void getKp(FindIterable<Document> allVorlesungen, List<Document> query) {
        for (Document d : allVorlesungen) {
            Integer kp = d.getInteger("KP");

            query.forEach(document -> {
                if (document.getObjectId("_id").equals(((Document) d.get("gelesenVon")).get("_id"))) {
                    Integer kpToChange = document.getInteger("KP");
                    kpToChange += kp;
                    document.replace("KP", kpToChange);
                }
            });
        }
    }
}

