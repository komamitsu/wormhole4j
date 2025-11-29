package org.komamitsu.wormhole4j;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import sun.misc.Unsafe;

class UnsafeUtils {
  static final int JAVA_VERSION;
  static final boolean IS_OPEN_J9;
  static final Unsafe UNSAFE;
  static final Class<?> _INNER_UNSAFE_CLASS;
  static final Object _INNER_UNSAFE;

  static final Lookup IMPL_LOOKUP;
  static volatile MethodHandle CONSTRUCTOR_LOOKUP;
  static volatile boolean CONSTRUCTOR_LOOKUP_ERROR;

  static {
    String property = System.getProperty("java.specification.version");
    if (property.startsWith("1.")) {
      property = property.substring(2);
    }
    String jmvName = System.getProperty("java.vm.name", "");
    IS_OPEN_J9 = jmvName.contains("OpenJ9");
    JAVA_VERSION = Integer.parseInt(property);
    Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      throw new UnsupportedOperationException("Unsafe is not supported in this platform.");
    }
    UNSAFE = unsafe;
    if (JAVA_VERSION >= 11) {
      try {
        Field theInternalUnsafeField = Unsafe.class.getDeclaredField("theInternalUnsafe");
        theInternalUnsafeField.setAccessible(true);
        _INNER_UNSAFE = theInternalUnsafeField.get(null);
        _INNER_UNSAFE_CLASS = _INNER_UNSAFE.getClass();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      _INNER_UNSAFE_CLASS = null;
      _INNER_UNSAFE = null;
    }

    Lookup trustedLookup = null;
    {
      try {
        Field implLookup = Lookup.class.getDeclaredField("IMPL_LOOKUP");
        long fieldOffset = UNSAFE.staticFieldOffset(implLookup);
        Object fieldBase = UNSAFE.staticFieldBase(implLookup);
        trustedLookup = (Lookup) UNSAFE.getObject(fieldBase, fieldOffset);
      } catch (Throwable ignored) {
        // ignored
      }
      if (trustedLookup == null) {
        trustedLookup = MethodHandles.lookup();
      }
      IMPL_LOOKUP = trustedLookup;
    }
  }

  private static final ClassValue<Lookup> lookupCache =
      new ClassValue<Lookup>() {
        @Override
        protected Lookup computeValue(Class type) {
          return _trustedLookup(type);
        }
      };

  public static Lookup _trustedLookup(Class<?> objectClass) {
    if (!CONSTRUCTOR_LOOKUP_ERROR) {
      try {
        int trusted = -1;
        MethodHandle constructor = CONSTRUCTOR_LOOKUP;
        if (JAVA_VERSION < 14) {
          if (constructor == null) {
            constructor =
                IMPL_LOOKUP.findConstructor(
                    Lookup.class, MethodType.methodType(void.class, Class.class, int.class));
            CONSTRUCTOR_LOOKUP = constructor;
          }
          int fullAccessMask = 31; // for IBM Open J9 JDK
          return (Lookup) constructor.invoke(objectClass, IS_OPEN_J9 ? fullAccessMask : trusted);
        } else {
          if (constructor == null) {
            constructor =
                IMPL_LOOKUP.findConstructor(
                    Lookup.class,
                    MethodType.methodType(void.class, Class.class, Class.class, int.class));
            CONSTRUCTOR_LOOKUP = constructor;
          }
          return (Lookup) constructor.invoke(objectClass, null, trusted);
        }
      } catch (Throwable ignored) {
        CONSTRUCTOR_LOOKUP_ERROR = true;
      }
    }
    if (JAVA_VERSION < 11) {
      Lookup lookup = getLookupByReflection(objectClass);
      if (lookup != null) {
        return lookup;
      }
    }
    return IMPL_LOOKUP.in(objectClass);
  }

  private static MethodHandles.Lookup getLookupByReflection(Class<?> cls) {
    try {
      Constructor<Lookup> constructor =
          MethodHandles.Lookup.class.getDeclaredConstructor(Class.class, int.class);
      constructor.setAccessible(true);
      return constructor.newInstance(
          cls, -1 // Lookup.TRUSTED
          );
    } catch (Exception e) {
      return null;
    }
  }

  static long objectFieldOffset(Field f) {
    return UNSAFE.objectFieldOffset(f);
  }

  static byte getByte(Object object, long offset) {
    return UNSAFE.getByte(object, offset);
  }

  static Object getObject(Object o, long offset) {
    return UNSAFE.getObject(o, offset);
  }
}
