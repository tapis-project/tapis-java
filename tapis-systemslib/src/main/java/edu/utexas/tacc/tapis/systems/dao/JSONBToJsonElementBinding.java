package edu.utexas.tacc.tapis.systems.dao;

import java.sql.SQLException;
import java.sql.Types;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.Gson;
import org.jooq.Binding;
import org.jooq.BindingGetResultSetContext;
import org.jooq.BindingGetSQLInputContext;
import org.jooq.BindingGetStatementContext;
import org.jooq.BindingRegisterContext;
import org.jooq.BindingSQLContext;
import org.jooq.BindingSetSQLOutputContext;
import org.jooq.BindingSetStatementContext;
import org.jooq.Converter;
import org.jooq.JSONB;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

// Based on example in jOOQ manual
// https://www.jooq.org/doc/3.13/manual/code-generation/custom-data-type-bindings/
// We're binding <T> = Object (unknown JDBC type), and <U> = JsonElement (user type)

// Bind Postgrsql jsonb column type to Gson JsonElement
public class JSONBToJsonElementBinding implements Binding<JSONB, JsonElement>
{
  // The converter does all the work
  @Override
  public Converter<JSONB, JsonElement> converter() {
    return new Converter<>() {
      @Override
      public JsonElement from(JSONB t) {
        return t == null ? JsonNull.INSTANCE : new Gson().fromJson(t.data(), JsonElement.class);
      }

      @Override
      public JSONB to(JsonElement u) {
        return u == null || u == JsonNull.INSTANCE ? null : JSONB.valueOf(new Gson().toJson(u));
      }

      @Override
      public Class<JSONB> fromType() { return JSONB.class; }

      @Override
      public Class<JsonElement> toType() { return JsonElement.class; }
    };
  }

    // Rendering a bind variable for the binding context's value and casting it to the jsonb type
    @Override
    public void sql(BindingSQLContext<JsonElement> ctx) throws SQLException {
      // Depending on how you generate your SQL, you may need to explicitly distinguish
      // between jOOQ generating bind variables or inlined literals.
      if (ctx.render().paramType() == ParamType.INLINED)
        ctx.render().visit(DSL.inline(ctx.convert(converter()).value())).sql("::jsonb");
      else
        ctx.render().sql("?::jsonb");
    }

    // Registering VARCHAR types for JDBC CallableStatement OUT parameters
    @Override
    public void register(BindingRegisterContext<JsonElement> ctx) throws SQLException {
      ctx.statement().registerOutParameter(ctx.index(), Types.VARCHAR);
    }

    // Converting the JsonElement to a String value and setting that on a JDBC PreparedStatement
    @Override
    public void set(BindingSetStatementContext<JsonElement> ctx) throws SQLException {
      ctx.statement().setString(ctx.index(), Objects.toString(ctx.convert(converter()).value(), null));
    }

    // Getting a String value from a JDBC ResultSet and converting that to a JsonElement
    @Override
    public void get(BindingGetResultSetContext<JsonElement> ctx) throws SQLException {
      ctx.convert(converter()).value(JSONB.valueOf(ctx.resultSet().getString(ctx.index())));
    }

    // Getting a String value from a JDBC CallableStatement and converting that to a JsonElement
    @Override
    public void get(BindingGetStatementContext<JsonElement> ctx) throws SQLException {
      ctx.convert(converter()).value(JSONB.valueOf(ctx.statement().getString(ctx.index())));
    }

    // Setting a value on a JDBC SQLOutput (useful for Oracle OBJECT types)
    @Override
    public void set(BindingSetSQLOutputContext<JsonElement> ctx) throws SQLException {
      throw new SQLFeatureNotSupportedException();
    }

    // Getting a value from a JDBC SQLInput (useful for Oracle OBJECT types)
    @Override
    public void get(BindingGetSQLInputContext<JsonElement> ctx) throws SQLException {
      throw new SQLFeatureNotSupportedException();
    }
  }