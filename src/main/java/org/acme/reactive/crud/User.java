package org.acme.reactive.crud;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

import java.util.stream.StreamSupport;

public class User {

    public Long id;

    public String name;
    public String password;
    public Long lat;
    public Long lng;
    public String userdata;



    public User() {
        // default constructo.
    }

    public User(String name, String password, Long lat, Long lng, String userdata) {
        this.name = name;
        this.password = password;
        this.lat = lat;
        this.lng = lng;
        this.userdata = userdata;
    }

    public User(Long id, String name, String password, Long lat, Long lng, String userdata) {
        this.id = id;
        this.name = name;
        this.password = password;
        this.lat = lat;
        this.lng = lng;
        this.userdata = userdata;
    }

    public static Multi<User> findAll(PgPool client) {
        return client.query("SELECT * FROM users ORDER BY name ASC").execute()
                // Create a Multi from the set of rows:
                .onItem().produceMulti(set -> Multi.createFrom().items(() -> StreamSupport.stream(set.spliterator(), false)))
                // For each row create a User instance
                .onItem().apply(User::from);
    }

    public static Uni<User> findById(PgPool client, Long id) {
        return client.preparedQuery("SELECT * FROM users WHERE id = $1").execute(Tuple.of(id))
                .onItem().apply(RowSet::iterator)
                .onItem().apply(iterator -> iterator.hasNext() ? from(iterator.next()) : null);
    }

    public static Uni<User> findByName(PgPool client, String name) {
        return client.preparedQuery("SELECT * FROM users WHERE name = $1").execute(Tuple.of(name))
                .onItem().apply(RowSet::iterator)
                .onItem().apply(iterator -> iterator.hasNext() ? from(iterator.next()) : null);
    }

    public Uni<Long> save(PgPool client) {
        return client.preparedQuery("INSERT INTO users (name, password, lat, lng, userdata) VALUES ($1, $2, $3, $4, $5) RETURNING (id)").execute(Tuple.of(name,password, lat, lng, userdata ))
                .onItem().apply(pgRowSet -> pgRowSet.iterator().next().getLong("id"));
    }

    public Uni<Boolean> update(PgPool client) {
        return client.preparedQuery("UPDATE users SET name = $1, password = $3, lat = $4, lng = $5, userdata = $6 WHERE name = $1").execute(Tuple.of(name, id, password, lat, lng,  userdata ))
                .onItem().apply(pgRowSet -> pgRowSet.rowCount() == 1);
    }

    public static Uni<Boolean> delete(PgPool client, Long id) {
        return client.preparedQuery("DELETE FROM users WHERE id = $1").execute(Tuple.of(id))
                .onItem().apply(pgRowSet -> pgRowSet.rowCount() == 1);
    }

    private static User from(Row row) {
        return new User(row.getLong("id"), row.getString("name"), row.getString("password"), row.getLong("lat"), row.getLong("lng"), row.getString("userdata"));
    }
}