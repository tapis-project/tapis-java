package edu.utexas.tacc.tapis.files.api.resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;



class Book {
    private long id;
    private String name;



    public void setName(String name) {
        this.name = name;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public Long getId() {
        return id;
    }
}



@Path("systems")
@Produces(MediaType.APPLICATION_JSON)
public class SystemsResource {

    @GET
    @ApiResponse(
            description = "List of messages."
    )
    public List<String> getSystems() throws WebApplicationException {

        List<String> results = new ArrayList<>();
        results.add("Hello");
        results.add("World");

        return results;
    }

    @GET
    @Path("{systemId}")
    public List<Book> getSystemByID(@PathParam("systemId") long systemId) throws WebApplicationException {
        Map<String, String> data = new HashMap<>();
        data.put("1", "abc");
        data.put("2", "def");
        data.put("3", "ghi");
//        return Response.status(Response.Status.OK).entity(data).build();
//        return data;
//
        List<Book> books = new ArrayList<>();
        Book b1 = new Book();
        b1.setId(1);
        b1.setName("Book One");
        books.add(b1);
        return books;
    }

}
