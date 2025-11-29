package org.komamitsu.wormhole4j;

import java.lang.reflect.Field;

final class StringUtils {
  private static final boolean STRING_VALUE_FIELD_IS_CHARS;
  private static final boolean STRING_VALUE_FIELD_IS_BYTES;

  private static final byte LATIN1 = 0;
  private static final byte UTF16 = 1;

  // Make offset compatible with graalvm native image.
  private static final long STRING_VALUE_FIELD_OFFSET;

  private static class Offset {
    // Make offset compatible with graalvm native image.
    private static final long STRING_CODER_FIELD_OFFSET;

    static {
      try {
        STRING_CODER_FIELD_OFFSET =
            UnsafeUtils.objectFieldOffset(String.class.getDeclaredField("coder"));
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static {
    Field valueField = getFieldNullable(String.class, "value");
    // Java8 string
    STRING_VALUE_FIELD_IS_CHARS = valueField != null && valueField.getType() == char[].class;
    // Java11 string
    STRING_VALUE_FIELD_IS_BYTES = valueField != null && valueField.getType() == byte[].class;
    try {
      // Make offset compatible with graalvm native image.
      STRING_VALUE_FIELD_OFFSET =
          UnsafeUtils.objectFieldOffset(String.class.getDeclaredField("value"));
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    /*
    // String length field for android.
    if (getFieldNullable(String.class, "count") == null) {
      throw new UnsupportedOperationException("Current jdk not supported");
    }
    if (getFieldNullable(String.class, "offset") == null) {
      throw new UnsupportedOperationException("Current jdk not supported");
    }
     */
  }

  private static Field getFieldNullable(Class<?> cls, String fieldName) {
    Class<?> clazz = cls;
    do {
      Field[] fields = clazz.getDeclaredFields();
      for (Field field : fields) {
        if (field.getName().equals(fieldName)) {
          return field;
        }
      }
      clazz = clazz.getSuperclass();
    } while (clazz != null);
    return null;
  }

  static boolean isCompactStringsSupported() {
    return STRING_VALUE_FIELD_IS_BYTES;
  }

  static byte[] getBytesFromString(String string) {
    if (STRING_VALUE_FIELD_IS_BYTES) {
      final byte coder = UnsafeUtils.getByte(string, Offset.STRING_CODER_FIELD_OFFSET);
      if (coder == LATIN1) {
        return (byte[]) UnsafeUtils.getObject(string, STRING_VALUE_FIELD_OFFSET);
      }
    }
    return string.getBytes();
  }

  static char[] getCharsFromString(String string) {
    if (STRING_VALUE_FIELD_IS_CHARS) {
      return (char[]) UnsafeUtils.getObject(string, STRING_VALUE_FIELD_OFFSET);
    }
    return string.toCharArray();
  }
}
