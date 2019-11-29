package io.smallrye.reactive.messaging.helpers;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.lang.reflect.*;
import java.net.URI;
import java.sql.Date;
import java.util.*;

import org.junit.Test;

@SuppressWarnings({ "unchecked", "unused", "rawtypes" })
public class TypeUtilsTest<B> {

    public interface This<K, V> {
    }

    class That<K, V> implements This<K, V> {
    }

    interface And<K, V> extends This<Number, Number> {
    }

    public class The<K, V> extends That<Number, Number> implements And<String, String> {
    }

    class Other<T> implements This<String, T> {
    }

    private class Thing<Q> extends Other<B> {
    }

    private class Tester implements This<String, B> {
    }

    @SuppressWarnings("WeakerAccess")
    public This<String, String> dis;

    @SuppressWarnings("WeakerAccess")
    public That<String, String> dat;

    @SuppressWarnings("WeakerAccess")
    public The<String, String> da;

    @SuppressWarnings("WeakerAccess")
    public Other<String> uhder;

    @SuppressWarnings("WeakerAccess")
    public Thing ding;

    @SuppressWarnings("WeakerAccess")
    public TypeUtilsTest<String>.Tester tester;

    @SuppressWarnings("WeakerAccess")
    public Tester tester2;

    @SuppressWarnings("WeakerAccess")
    public TypeUtilsTest<String>.That<String, String> dat2;

    @SuppressWarnings("WeakerAccess")
    public TypeUtilsTest<Number>.That<String, String> dat3;

    @SuppressWarnings("WeakerAccess")
    public Comparable<? extends Integer>[] intWildcardComparable = null;

    public static Comparable<String> stringComparable;

    public static Comparable<URI> uriComparable;

    @SuppressWarnings("WeakerAccess")
    public static Comparable<Integer> intComparable;

    @SuppressWarnings("WeakerAccess")
    public static Comparable<Long> longComparable;

    @SuppressWarnings("WeakerAccess")
    public static Comparable<?> wildcardComparable;

    public static URI uri;

    public static List<String>[] stringListArray;

    @SuppressWarnings("WeakerAccess")
    public void dummyMethod(final List list0, final List<Object> list1, final List<?> list2,
            final List<? super Object> list3, final List<String> list4, final List<? extends String> list5,
            final List<? super String> list6, final List[] list7, final List<Object>[] list8, final List<?>[] list9,
            final List<? super Object>[] list10, final List<String>[] list11, final List<? extends String>[] list12,
            final List<? super String>[] list13) {
    }

    @SuppressWarnings({ "boxing", "JoinDeclarationAndAssignmentJava", "UnusedAssignment" })
    @Test
    public void testIsAssignable() throws SecurityException, NoSuchMethodException,
            NoSuchFieldException {
        List list0 = null;
        List<Object> list1;
        List<Object> list3;
        List<String> list4;
        List<? extends String> list5;
        List[] list7 = null;
        List<Object>[] list8;
        List<?>[] list9;
        List<? super Object>[] list10;
        List<String>[] list11;
        List<? extends String>[] list12;
        List<? super String>[] list13;
        final Class<?> clazz = getClass();
        final Method method = clazz.getMethod("dummyMethod", List.class, List.class, List.class,
                List.class, List.class, List.class, List.class, List[].class, List[].class,
                List[].class, List[].class, List[].class, List[].class, List[].class);
        final Type[] types = method.getGenericParameterTypes();
        delegateBooleanAssertion(types, 0, 0, true);
        delegateBooleanAssertion(types, 0, 1, true);
        delegateBooleanAssertion(types, 1, 0, true);
        delegateBooleanAssertion(types, 0, 2, true);
        delegateBooleanAssertion(types, 2, 0, true);
        delegateBooleanAssertion(types, 0, 3, true);
        delegateBooleanAssertion(types, 3, 0, true);
        delegateBooleanAssertion(types, 0, 4, true);
        delegateBooleanAssertion(types, 4, 0, true);
        delegateBooleanAssertion(types, 0, 5, true);
        delegateBooleanAssertion(types, 5, 0, true);
        delegateBooleanAssertion(types, 0, 6, true);
        delegateBooleanAssertion(types, 6, 0, true);
        delegateBooleanAssertion(types, 1, 1, true);
        delegateBooleanAssertion(types, 1, 2, true);
        delegateBooleanAssertion(types, 2, 1, false);
        delegateBooleanAssertion(types, 1, 3, true);
        delegateBooleanAssertion(types, 3, 1, false);
        delegateBooleanAssertion(types, 1, 4, false);
        delegateBooleanAssertion(types, 4, 1, false);
        delegateBooleanAssertion(types, 1, 5, false);
        delegateBooleanAssertion(types, 5, 1, false);
        delegateBooleanAssertion(types, 1, 6, true);
        delegateBooleanAssertion(types, 6, 1, false);
        delegateBooleanAssertion(types, 2, 2, true);
        delegateBooleanAssertion(types, 2, 3, false);
        delegateBooleanAssertion(types, 3, 2, true);
        delegateBooleanAssertion(types, 2, 4, false);
        delegateBooleanAssertion(types, 4, 2, true);
        delegateBooleanAssertion(types, 2, 5, false);
        delegateBooleanAssertion(types, 5, 2, true);
        delegateBooleanAssertion(types, 2, 6, false);
        delegateBooleanAssertion(types, 6, 2, true);
        delegateBooleanAssertion(types, 3, 3, true);
        delegateBooleanAssertion(types, 3, 4, false);
        delegateBooleanAssertion(types, 4, 3, false);
        delegateBooleanAssertion(types, 3, 5, false);
        delegateBooleanAssertion(types, 5, 3, false);
        delegateBooleanAssertion(types, 3, 6, true);
        delegateBooleanAssertion(types, 6, 3, false);
        delegateBooleanAssertion(types, 4, 4, true);
        delegateBooleanAssertion(types, 4, 5, true);
        delegateBooleanAssertion(types, 5, 4, false);
        delegateBooleanAssertion(types, 4, 6, true);
        delegateBooleanAssertion(types, 6, 4, false);
        delegateBooleanAssertion(types, 5, 5, true);
        delegateBooleanAssertion(types, 5, 6, false);
        delegateBooleanAssertion(types, 6, 5, false);
        delegateBooleanAssertion(types, 6, 6, true);
        delegateBooleanAssertion(types, 7, 7, true);
        delegateBooleanAssertion(types, 7, 8, true);
        delegateBooleanAssertion(types, 8, 7, true);
        delegateBooleanAssertion(types, 7, 9, true);
        delegateBooleanAssertion(types, 9, 7, true);
        delegateBooleanAssertion(types, 7, 10, true);
        delegateBooleanAssertion(types, 10, 7, true);
        delegateBooleanAssertion(types, 7, 11, true);
        delegateBooleanAssertion(types, 11, 7, true);
        delegateBooleanAssertion(types, 7, 12, true);
        delegateBooleanAssertion(types, 12, 7, true);
        delegateBooleanAssertion(types, 7, 13, true);
        delegateBooleanAssertion(types, 13, 7, true);
        delegateBooleanAssertion(types, 8, 8, true);
        delegateBooleanAssertion(types, 8, 9, true);
        delegateBooleanAssertion(types, 9, 8, false);
        delegateBooleanAssertion(types, 8, 10, true);
        delegateBooleanAssertion(types, 10, 8, false);
        delegateBooleanAssertion(types, 8, 11, false);
        delegateBooleanAssertion(types, 11, 8, false);
        delegateBooleanAssertion(types, 8, 12, false);
        delegateBooleanAssertion(types, 12, 8, false);
        delegateBooleanAssertion(types, 8, 13, true);
        delegateBooleanAssertion(types, 13, 8, false);
        delegateBooleanAssertion(types, 9, 9, true);
        delegateBooleanAssertion(types, 9, 10, false);
        delegateBooleanAssertion(types, 10, 9, true);
        delegateBooleanAssertion(types, 9, 11, false);
        delegateBooleanAssertion(types, 11, 9, true);
        delegateBooleanAssertion(types, 9, 12, false);
        delegateBooleanAssertion(types, 12, 9, true);
        delegateBooleanAssertion(types, 9, 13, false);
        delegateBooleanAssertion(types, 13, 9, true);
        delegateBooleanAssertion(types, 10, 10, true);
        delegateBooleanAssertion(types, 10, 11, false);
        delegateBooleanAssertion(types, 11, 10, false);
        delegateBooleanAssertion(types, 10, 12, false);
        delegateBooleanAssertion(types, 12, 10, false);
        delegateBooleanAssertion(types, 10, 13, true);
        delegateBooleanAssertion(types, 13, 10, false);
        delegateBooleanAssertion(types, 11, 11, true);
        delegateBooleanAssertion(types, 11, 12, true);
        delegateBooleanAssertion(types, 12, 11, false);
        delegateBooleanAssertion(types, 11, 13, true);
        delegateBooleanAssertion(types, 13, 11, false);
        delegateBooleanAssertion(types, 12, 12, true);
        delegateBooleanAssertion(types, 12, 13, false);
        delegateBooleanAssertion(types, 13, 12, false);
        delegateBooleanAssertion(types, 13, 13, true);
        final Type disType = getClass().getField("dis").getGenericType();
        final Type datType = getClass().getField("dat").getGenericType();
        final Type daType = getClass().getField("da").getGenericType();
        final Type uhderType = getClass().getField("uhder").getGenericType();
        final Type dingType = getClass().getField("ding").getGenericType();
        final Type testerType = getClass().getField("tester").getGenericType();
        final Type tester2Type = getClass().getField("tester2").getGenericType();
        final Type dat2Type = getClass().getField("dat2").getGenericType();
        final Type dat3Type = getClass().getField("dat3").getGenericType();
        dis = dat;
        assertTrue(TypeUtils.isAssignable(datType, disType));
        assertFalse(TypeUtils.isAssignable(daType, disType));
        dis = uhder;
        assertTrue(TypeUtils.isAssignable(uhderType, disType));
        dis = ding;
        assertFalse(TypeUtils.isAssignable(dingType, disType));
        dis = tester;
        assertTrue(TypeUtils.isAssignable(testerType, disType));
        assertFalse(TypeUtils.isAssignable(tester2Type, disType));
        assertFalse(TypeUtils.isAssignable(dat2Type, datType));
        assertFalse(TypeUtils.isAssignable(datType, dat2Type));
        assertFalse(TypeUtils.isAssignable(dat3Type, datType));
        final char ch = 0;
        final boolean bo = false;
        final byte by = 0;
        final short sh = 0;
        int in = 0;
        long lo = 0;
        final float fl = 0;
        double du = 0;
        du = ch;
        assertTrue(TypeUtils.isAssignable(char.class, double.class));
        du = by;
        assertTrue(TypeUtils.isAssignable(byte.class, double.class));
        du = sh;
        assertTrue(TypeUtils.isAssignable(short.class, double.class));
        du = in;
        assertTrue(TypeUtils.isAssignable(int.class, double.class));
        assertTrue(TypeUtils.isAssignable(long.class, double.class));
        du = fl;
        assertTrue(TypeUtils.isAssignable(float.class, double.class));
        assertTrue(TypeUtils.isAssignable(int.class, long.class));
        assertTrue(TypeUtils.isAssignable(Integer.class, long.class));
        assertFalse(TypeUtils.isAssignable(int.class, Long.class));
        assertFalse(TypeUtils.isAssignable(Integer.class, Long.class));
        assertTrue(TypeUtils.isAssignable(Integer.class, int.class));
        assertTrue(TypeUtils.isAssignable(int.class, Integer.class));
        assertTrue(TypeUtils.isAssignable(int.class, Number.class));
        assertTrue(TypeUtils.isAssignable(int.class, Object.class));
        final Type intComparableType = getClass().getField("intComparable").getGenericType();
        intComparable = 1;
        assertTrue(TypeUtils.isAssignable(int.class, intComparableType));
        assertTrue(TypeUtils.isAssignable(int.class, Comparable.class));
        final Serializable ser = 1;
        assertTrue(TypeUtils.isAssignable(int.class, Serializable.class));
        final Type longComparableType = getClass().getField("longComparable").getGenericType();
        assertFalse(TypeUtils.isAssignable(int.class, longComparableType));
        assertFalse(TypeUtils.isAssignable(Integer.class, longComparableType));
        assertFalse(TypeUtils.isAssignable(int[].class, long[].class));
        final Integer[] ia = null;
        final Type caType = getClass().getField("intWildcardComparable").getGenericType();
        assertTrue(TypeUtils.isAssignable(Integer[].class, caType));
        assertFalse(TypeUtils.isAssignable(Integer[].class, int[].class));
        final int[] ina = null;
        Object[] oa;
        assertFalse(TypeUtils.isAssignable(int[].class, Object[].class));
        oa = new Integer[0];
        assertTrue(TypeUtils.isAssignable(Integer[].class, Object[].class));
        final Type bClassType = AClass.class.getField("bClass").getGenericType();
        final Type cClassType = AClass.class.getField("cClass").getGenericType();
        final Type dClassType = AClass.class.getField("dClass").getGenericType();
        final Type eClassType = AClass.class.getField("eClass").getGenericType();
        final Type fClassType = AClass.class.getField("fClass").getGenericType();
        final AClass aClass = new AClass(new AAClass<>());
        aClass.bClass = aClass.cClass;
        assertTrue(TypeUtils.isAssignable(cClassType, bClassType));
        aClass.bClass = aClass.dClass;
        assertTrue(TypeUtils.isAssignable(dClassType, bClassType));
        aClass.bClass = aClass.eClass;
        assertTrue(TypeUtils.isAssignable(eClassType, bClassType));
        aClass.bClass = aClass.fClass;
        assertTrue(TypeUtils.isAssignable(fClassType, bClassType));
        aClass.cClass = aClass.dClass;
        assertTrue(TypeUtils.isAssignable(dClassType, cClassType));
        aClass.cClass = aClass.eClass;
        assertTrue(TypeUtils.isAssignable(eClassType, cClassType));
        aClass.cClass = aClass.fClass;
        assertTrue(TypeUtils.isAssignable(fClassType, cClassType));
        aClass.dClass = aClass.eClass;
        assertTrue(TypeUtils.isAssignable(eClassType, dClassType));
        aClass.dClass = aClass.fClass;
        assertTrue(TypeUtils.isAssignable(fClassType, dClassType));
        aClass.eClass = aClass.fClass;
        assertTrue(TypeUtils.isAssignable(fClassType, eClassType));
    }

    private void delegateBooleanAssertion(final Type[] types, final int i2, final int i1, final boolean expected) {
        final Type type1 = types[i1];
        final Type type2 = types[i2];
        final boolean isAssignable = TypeUtils.isAssignable(type2, type1);

        if (expected) {
            assertTrue(isAssignable);
        } else {
            assertFalse(isAssignable);
        }
    }

    @SuppressWarnings("UnusedAssignment")
    @Test
    public void testGetTypeArguments() {
        Map<TypeVariable<?>, Type> typeVarAssigns;
        TypeVariable<?> treeSetTypeVar;
        Type typeArg;

        typeVarAssigns = TypeUtils.getTypeArguments(Integer.class, Comparable.class);
        treeSetTypeVar = Comparable.class.getTypeParameters()[0];
        assertTrue(typeVarAssigns.containsKey(treeSetTypeVar));
        typeArg = typeVarAssigns.get(treeSetTypeVar);
        assertEquals(Integer.class, typeVarAssigns.get(treeSetTypeVar));

        typeVarAssigns = TypeUtils.getTypeArguments(int.class, Comparable.class);
        treeSetTypeVar = Comparable.class.getTypeParameters()[0];
        assertTrue(typeVarAssigns.containsKey(treeSetTypeVar));
        typeArg = typeVarAssigns.get(treeSetTypeVar);
        assertEquals(Integer.class, typeVarAssigns.get(treeSetTypeVar));

        @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
        final Collection<Integer> col = Arrays.asList(new Integer[0]);
        typeVarAssigns = TypeUtils.getTypeArguments(List.class, Collection.class);
        treeSetTypeVar = Comparable.class.getTypeParameters()[0];
        assertFalse(typeVarAssigns.containsKey(treeSetTypeVar));

        typeVarAssigns = TypeUtils.getTypeArguments(AAAClass.BBBClass.class, AAClass.BBClass.class);
        assertEquals(2, typeVarAssigns.size());
        assertEquals(String.class, typeVarAssigns.get(AAClass.class.getTypeParameters()[0]));
        assertEquals(String.class, typeVarAssigns.get(AAClass.BBClass.class.getTypeParameters()[0]));

        typeVarAssigns = TypeUtils.getTypeArguments(Other.class, This.class);
        assertEquals(2, typeVarAssigns.size());
        assertEquals(String.class, typeVarAssigns.get(This.class.getTypeParameters()[0]));
        assertEquals(Other.class.getTypeParameters()[0], typeVarAssigns.get(This.class.getTypeParameters()[1]));

        typeVarAssigns = TypeUtils.getTypeArguments(And.class, This.class);
        assertEquals(2, typeVarAssigns.size());
        assertEquals(Number.class, typeVarAssigns.get(This.class.getTypeParameters()[0]));
        assertEquals(Number.class, typeVarAssigns.get(This.class.getTypeParameters()[1]));

        typeVarAssigns = TypeUtils.getTypeArguments(Thing.class, Other.class);
        assertEquals(2, typeVarAssigns.size());
        assertEquals(getClass().getTypeParameters()[0], typeVarAssigns.get(getClass().getTypeParameters()[0]));
        assertEquals(getClass().getTypeParameters()[0], typeVarAssigns.get(Other.class.getTypeParameters()[0]));
    }

    @Test
    public void testNormalizeUpperBounds() {
        final Type[] typeArray = { String.class, String.class };
        final Type[] expectedArray = { String.class };
        assertArrayEquals(expectedArray, TypeUtils.normalizeUpperBounds(typeArray));
    }

    @Test
    public void testWildcardType() throws Exception {
        final WildcardType simpleWildcard = new WildcardTypeImpl(new Type[] { String.class }, null);
        final Field cClass = AClass.class.getField("cClass");
        assertTrue(TypeUtils.equals(((ParameterizedType) cClass.getGenericType()).getActualTypeArguments()[0],
                simpleWildcard));
        assertEquals(String.format("? extends %s", String.class.getName()), TypeUtils.toString(simpleWildcard));
        assertEquals(String.format("? extends %s", String.class.getName()), simpleWildcard.toString());
    }

    @Test
    public void testUnboundedWildcardType() {
        final WildcardType unbounded = new WildcardTypeImpl(null, null);
        assertArrayEquals(new Type[] { Object.class }, TypeUtils.getImplicitUpperBounds(unbounded));
        assertArrayEquals(new Type[] { null }, TypeUtils.getImplicitLowerBounds(unbounded));
        assertEquals("?", TypeUtils.toString(unbounded));
        assertEquals("?", unbounded.toString());
    }

    @Test
    public void testLowerBoundedWildcardType() {
        final WildcardType lowerBounded = new WildcardTypeImpl(null, new Type[] { Date.class });
        assertEquals(String.format("? super %s", java.sql.Date.class.getName()), TypeUtils.toString(lowerBounded));
        assertEquals(String.format("? super %s", java.sql.Date.class.getName()), lowerBounded.toString());

        final TypeVariable<Class<Iterable>> iterableT0 = Iterable.class.getTypeParameters()[0];
        final WildcardType lowerTypeVariable = new WildcardTypeImpl(null, new Type[] { iterableT0 });
        assertEquals(String.format("? super %s", iterableT0.getName()), TypeUtils.toString(lowerTypeVariable));
        assertEquals(String.format("? super %s", iterableT0.getName()), lowerTypeVariable.toString());
    }

    @Test
    public void testLang1114() throws Exception {
        final Type nonWildcardType = getClass().getDeclaredField("wildcardComparable").getGenericType();
        final Type wildcardType = ((ParameterizedType) nonWildcardType).getActualTypeArguments()[0];

        assertFalse(TypeUtils.equals(wildcardType, nonWildcardType));
        assertFalse(TypeUtils.equals(nonWildcardType, wildcardType));
    }

    @Test
    public void testToLongString() {
        assertEquals(getClass().getName() + ":B", TypeUtils.toLongString(getClass().getTypeParameters()[0]));
    }

    public static class ClassWithSuperClassWithGenericType extends ArrayList<Object> {
        private static final long serialVersionUID = 1L;

        static <U> Iterable<U> methodWithGenericReturnType() {
            return null;
        }
    }

    @Test
    public void testFailingWildcard() throws Exception {
        final Type fromType = ClassWithSuperClassWithGenericType.class.getDeclaredMethod("methodWithGenericReturnType")
                .getGenericReturnType();
        final WildcardType failingToType = new WildcardTypeImpl(null, new Type[] { ClassWithSuperClassWithGenericType.class });
        assertTrue(TypeUtils.isAssignable(fromType, failingToType));
    }

    @Test
    public void testLANG1348() throws Exception {
        final Method method = Enum.class.getMethod("valueOf", Class.class, String.class);
        assertEquals("T extends java.lang.Enum<T>", TypeUtils.toString(method.getGenericReturnType()));
    }

    public Iterable<? extends Map<Integer, ? extends Collection<?>>> iterable;

    public static <G extends Comparable<G>> G stub() {
        return null;
    }

    public static <G extends Comparable<? super G>> G stub2() {
        return null;
    }

    public static <T extends Comparable<? extends T>> T stub3() {
        return null;
    }
}

@SuppressWarnings("ALL")
class AAClass<T> {

    public class BBClass<S> {
    }
}

class AAAClass extends AAClass<String> {
    class BBBClass extends BBClass<String> {
    }
}

@SuppressWarnings({ "rawtypes", "unused", "WeakerAccess" })
class AClass extends AAClass<String>.BBClass<Number> {

    AClass(final AAClass<String> enclosingInstance) {
        enclosingInstance.super();
    }

    public class BClass<T> {
    }

    public class CClass<T> extends BClass {
    }

    public class DClass<T> extends CClass<T> {
    }

    public class EClass<T> extends DClass {
    }

    public class FClass extends EClass<String> {
    }

    public class GClass<T extends BClass<? extends T> & AInterface<AInterface<? super T>>> {
    }

    public BClass<Number> bClass;

    public CClass<? extends String> cClass;

    public DClass<String> dClass;

    public EClass<String> eClass;

    public FClass fClass;

    public GClass gClass;

    public interface AInterface<T> {
    }
}
