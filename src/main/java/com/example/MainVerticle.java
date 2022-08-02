package com.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.mssqlclient.MSSQLConnectOptions;
import io.vertx.mssqlclient.MSSQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public class MainVerticle extends AbstractVerticle {

	@Override
	public void start(Promise<Void> startPromise) throws Exception {

		JsonObject setting = new JsonObject(
				vertx.fileSystem().readFileBlocking("setting.json"));

		int SeverPort = (Integer) setting.getValue("port");
		JsonObject dbConn = (JsonObject) setting.getJsonObject("dbConnection");
		String host = dbConn.getString("dbHost");
		String db = dbConn.getString("dbBase");
		String user = dbConn.getString("dbUser");
		String pass = dbConn.getString("dbPass");
		int port = 1433;

		System.out.println("DBConnection: " + host);

		Router router = Router.router(vertx);

		router.get("/").handler(ctx -> {
			ctx.response().putHeader("Content-Type", "text/html")
					.end("halo dunia...!<br/><a href='/baca'>klik disini</a>");
		});

		router.post("/test5").handler(ctx -> {
			ctx.request().bodyHandler(bodyHandler -> {
				JsonObject body = bodyHandler.toJsonObject();
				System.out.println(body.toString());
				System.out.println(body.getString("tipe"));
				body.put("warna", "bronze");
				System.out.println(body.getValue("waktu"));
				ctx.response().putHeader("content-type", "application/json").end(body.toString());
			});
		});

		router.get("/baca").handler(ctx -> {

			MSSQLConnectOptions connectOptions = new MSSQLConnectOptions()
					.setPort(port)
					.setHost(host)
					.setDatabase(db)
					.setUser(user)
					.setPassword(pass);
			PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
			MSSQLPool client = MSSQLPool.pool(vertx, connectOptions, poolOptions);

			client
					.query("SELECT * FROM Employees;")
					.execute(ar -> {
						if (ar.succeeded()) {
							RowSet<Row> result = ar.result();
							System.out.println("Got " + result.size() + " rows ");
							// Employee employee = new Employee();

							JsonArray arr = new JsonArray();
							for (Row row : result) {
								JsonObject obj = new JsonObject();
								obj.put("Id", (Integer) row.getValue("ID"));
								obj.put("Name", row.getString("Name"));
								obj.put("Location", row.getString("Location"));
								obj.put("Waktu", row.getLocalDateTime("Waktu").toString());
								arr.add(obj);
							}

							ctx.response().putHeader("content-type", "application/json").end(arr.toString());
						} else {
							System.out.println("Failure: " + ar.cause().getMessage());
							ctx.response().end("Failure: " + ar.cause().getMessage());
						}

						client.close();
					});

			// ctx.response().end("Ok: ");
		});

		router.get("/bacasatu/:id").handler(ctx -> {
			String id = ctx.pathParam("id");

			MSSQLConnectOptions connectOptions = new MSSQLConnectOptions()
					.setPort(port)
					.setHost(host)
					.setDatabase(db)
					.setUser(user)
					.setPassword(pass);
			PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
			MSSQLPool client = MSSQLPool.pool(vertx, connectOptions, poolOptions);

			client
					.query("SELECT * FROM Employees where id=" + id + ";")
					.execute(ar -> {
						if (ar.succeeded()) {
							RowSet<Row> result = ar.result();
							Row row = result.iterator().next();

							JsonObject obj = new JsonObject();
							obj.put("Id", (Integer) row.getValue("ID"));
							obj.put("Name", row.getString("Name"));
							obj.put("Location", row.getString("Location"));
							obj.put("Waktu", row.getLocalDateTime("Waktu").toString());

							ctx.response().putHeader("content-type", "application/json").end(obj.toString());
						} else {
							System.out.println("Failure: " + ar.cause().getMessage());
							ctx.response().end("Failure: " + ar.cause().getMessage());
						}
						client.close();
					});

		});

		vertx.createHttpServer().requestHandler(router)
				.listen(SeverPort, http -> {
					if (http.succeeded()) {
						startPromise.complete();
						System.out.println("HTTP server started on port " + SeverPort);
					} else {
						startPromise.fail(http.cause());
					}
				});
	}
}
