package com.hubspot.singularity.data;

import static java.lang.String.format;

import java.io.IOException;

public class SandboxException extends IOException {
  public SandboxException(String format, Object... args) {
    super(format(format, args));
  }

  public SandboxException(Exception e) {
    super(e);
  }
}
