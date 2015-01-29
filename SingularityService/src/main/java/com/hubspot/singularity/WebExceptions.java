package com.hubspot.singularity;

import static java.lang.String.format;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.Status.Family;
import javax.ws.rs.core.Response.StatusType;

import com.sun.jersey.api.ConflictException;
import com.sun.jersey.api.NotFoundException;

public final class WebExceptions {

  public static enum WebStatusCode implements StatusType {
    TIMEOUT(408, "Request timeout");

    private final int statusCode;
    private final String reasonPhrase;
    private final Family family;

    private WebStatusCode(final int statusCode, final String reasonPhrase) {
      this.statusCode = statusCode;
      this.reasonPhrase = reasonPhrase;
      switch (statusCode / 100) {
      case 1:
        this.family = Family.INFORMATIONAL;
        break;
      case 2:
        this.family = Family.SUCCESSFUL;
        break;
      case 3:
        this.family = Family.REDIRECTION;
        break;
      case 4:
        this.family = Family.CLIENT_ERROR;
        break;
      case 5:
        this.family = Family.SERVER_ERROR;
        break;
      default:
        this.family = Family.OTHER;
        break;
      }
    }

    @Override
    public int getStatusCode() {
      return statusCode;
    }

    @Override
    public Family getFamily() {
      return family;
    }

    @Override
    public String getReasonPhrase() {
      return reasonPhrase;
    }
  }

  private WebExceptions() {
  }

  public static void checkBadRequest(boolean condition, String message, Object... args) {
    if (!condition) {
      badRequest(message, args);
    }
  }

  public static void checkConflict(boolean condition, String message, Object... args) {
    if (!condition) {
      conflict(message, args);
    }
  }

  public static void checkNotFound(boolean condition, String message, Object... args) {
    if (!condition) {
      notFound(message, args);
    }
  }

  public static void checkForbidden(boolean condition, String message, Object... args) {
    if (!condition) {
      forbidden(message, args);
    }
  }

  public static <T> T checkNotNullBadRequest(T value, String message, Object... args) {
    if (value == null) {
      badRequest(message, args);
    }
    return value;
  }

  public static WebApplicationException badRequest(String message, Object... args) {
    throw webException(Status.BAD_REQUEST, message, args);
  }

  public static WebApplicationException timeout(String message, Object... args) {
    throw webException(WebStatusCode.TIMEOUT, message, args);
  }

  public static WebApplicationException conflict(String message, Object... args) {
    if (args.length > 0) {
      message = format(message, args);
    }
    throw new ConflictException(message);
  }

  public static WebApplicationException notFound(String message, Object... args) {
    if (args.length > 0) {
      message = format(message, args);
    }
    throw new NotFoundException(message);
  }

  public static WebApplicationException serverError(Throwable t, String message, Object... args) {
    if (args.length > 0) {
      message = format(message, args);
    }
    throw webException(t, Status.INTERNAL_SERVER_ERROR, message, args);
  }

  public static WebApplicationException forbidden(String message, Object... args) {
    return webException(Status.FORBIDDEN, message, args);
  }

  private static WebApplicationException webException(StatusType statusCode, String message, Object... formatArgs) {
    if (formatArgs.length > 0) {
      message = format(message, formatArgs);
    }

    throw new WebApplicationException(Response.status(statusCode).entity(message).type("text/plain").build());
  }

  private static WebApplicationException webException(Throwable t, StatusType statusCode, String message, Object... formatArgs) {
    if (formatArgs.length > 0) {
      message = format(message, formatArgs);
    }

    throw new WebApplicationException(t, Response.status(statusCode).entity(message).type("text/plain").build());
  }
}
