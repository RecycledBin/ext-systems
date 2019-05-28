package edu.javacourse.city.dao;

import edu.javacourse.city.domain.PersonRequest;
import edu.javacourse.city.domain.PersonResponse;
import edu.javacourse.city.exception.PersonCheckException;

import java.sql.*;

public class PersonCheckDao
{
    private static final String SQL_REQUEST =
            "SELECT temporal, UPPER(p.sur_name) FROM cr_address_person ap " +
                "INNER JOIN cr_person p ON p.person_id = ap.person_id " +
                "INNER JOIN cr_address a ON a.address_id = ap.address_id " +
                "WHERE " +
                "CURRENT_DATE >= ap.start_date " +
                "  AND (CURRENT_DATE <= ap.end_date OR ap.end_date IS NULL)" +
                "  AND UPPER(p.sur_name) = UPPER(? COLLATE \"en_US\") " +
                "  AND UPPER(p.given_name) = UPPER(? COLLATE \"en_US\") " +
                "  AND UPPER(p.patronymic) = UPPER(? COLLATE \"en_US\") " +
                "  AND p.date_of_birth = ? AND a.street_code = ? " +
                "  AND UPPER(a.building) = UPPER(? COLLATE \"en_US\") ";

    public PersonResponse checkPerson(PersonRequest request) throws PersonCheckException {
        PersonResponse response = new PersonResponse();

        String sql = SQL_REQUEST;
        if (request.getExtension() != null) {
            sql += "  AND UPPER(a.extension) = UPPER(? COLLATE \"en_US\") ";
        } else {
            sql += " AND a.extension IS NULL ";
        }
        if (request.getApartment() != null) {
            sql += "  AND UPPER(a.apartment) = UPPER(? COLLATE \"en_US\")";
        } else {
            sql += "AND a.apartment IS NULL";
        }

        try (Connection con = getConnection();
             PreparedStatement stmt = con.prepareStatement(sql)) {

            int count = 1;
            stmt.setString(count++, request.getSurName());
            stmt.setString(count++, request.getGivenName());
            stmt.setString(count++, request.getPatronymic());
            stmt.setDate(count++, java.sql.Date.valueOf(request.getDateOfBirth()));
            stmt.setInt(count++, request.getStreetCode());
            stmt.setString(count++, request.getBuilding());
            if (request.getExtension() != null) stmt.setString(count++, request.getExtension());
            if (request.getApartment() != null) stmt.setString(count++, request.getApartment());

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                response.setRegistered(true);
                response.setTemporal(rs.getBoolean("temporal"));
            }

        } catch(SQLException ex) {
            throw new PersonCheckException(ex);
        }

        return response;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost/city_register",
                "postgres", "postgres");
    }
}
