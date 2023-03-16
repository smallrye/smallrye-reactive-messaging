package io.smallrye.reactive.messaging.providers.helpers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;

import java.lang.reflect.*;
import java.util.*;

/**
 * Utility methods focusing on type inspection, particularly with regard to
 * generics.
 *
 * Code extracted from Apache Commons Lang 3 - TypeUtils.java and ArrayUtils.java
 */
public class TypeUtils {

    private TypeUtils() {
        // Avoid direct instantiation.
    }

    /**
     * <p>
     * Checks if the subject type may be implicitly cast to the target type
     * following the Java generics rules.
     *
     * @param type the subject type to be assigned to the target type
     * @param toType the target type
     * @return {@code true} if {@code type} is assignable to {@code toType}.
     */
    public static boolean isAssignable(final Type type, final Type toType) {
        return isAssignable(type, toType, null);
    }

    /**
     * <p>
     * Checks if the subject type may be implicitly cast to the target type
     * following the Java generics rules.
     * </p>
     *
     * @param type the subject type to be assigned to the target type
     * @param toType the target type
     * @param typeVarAssigns optional map of type variable assignments
     * @return {@code true} if {@code type} is assignable to {@code toType}.
     */
    private static boolean isAssignable(final Type type, final Type toType,
            final Map<TypeVariable<?>, Type> typeVarAssigns) {
        if (toType == null || toType instanceof Class<?>) {
            return isAssignable(type, (Class<?>) toType);
        }

        if (toType instanceof ParameterizedType) {
            return isAssignable(type, (ParameterizedType) toType, typeVarAssigns);
        }

        if (toType instanceof GenericArrayType) {
            return isAssignable(type, (GenericArrayType) toType, typeVarAssigns);
        }

        if (toType instanceof WildcardType) {
            return isAssignable(type, (WildcardType) toType, typeVarAssigns);
        }

        if (toType instanceof TypeVariable<?>) {
            return isAssignable(type, (TypeVariable<?>) toType);
        }
        throw ex.illegalStateUnhandledType(toType);
    }

    /**
     * <p>
     * Checks if the subject type may be implicitly cast to the target class
     * following the Java generics rules.
     * </p>
     *
     * @param type the subject type to be assigned to the target type
     * @param toClass the target class
     * @return {@code true} if {@code type} is assignable to {@code toClass}.
     */
    private static boolean isAssignable(final Type type, final Class<?> toClass) {
        if (type == null) {
            return toClass == null || !toClass.isPrimitive();
        }

        // only a null type can be assigned to null type which
        // would have cause the previous to return true
        if (toClass == null) {
            return false;
        }

        // all types are assignable to themselves
        if (toClass.equals(type)) {
            return true;
        }

        if (type instanceof Class<?>) {
            return ClassUtils.isAssignable((Class<?>) type, toClass);
        }

        if (type instanceof ParameterizedType) {
            // only have to compare the raw type to the class
            return isAssignable(getRawType((ParameterizedType) type), toClass);
        }

        if (type instanceof TypeVariable<?>) {
            // if any of the bounds are assignable to the class, then the
            // type is assignable to the class.
            for (final Type bound : ((TypeVariable<?>) type).getBounds()) {
                if (isAssignable(bound, toClass)) {
                    return true;
                }
            }

            return false;
        }

        // the only classes to which a generic array type can be assigned
        // are class Object and array classes
        if (type instanceof GenericArrayType) {
            return toClass.equals(Object.class)
                    || toClass.isArray()
                            && isAssignable(((GenericArrayType) type).getGenericComponentType(), toClass
                                    .getComponentType());
        }

        // wildcard types are not assignable to a class (though one would think
        // "? super Object" would be assignable to Object)
        if (type instanceof WildcardType) {
            return false;
        }

        throw ex.illegalStateUnhandledType(type);
    }

    /**
     * <p>
     * Checks if the subject type may be implicitly cast to the target
     * parameterized type following the Java generics rules.
     * </p>
     *
     * @param type the subject type to be assigned to the target type
     * @param toParameterizedType the target parameterized type
     * @param typeVarAssigns a map with type variables
     * @return {@code true} if {@code type} is assignable to {@code toType}.
     */
    static boolean isAssignable(final Type type, final ParameterizedType toParameterizedType,
            final Map<TypeVariable<?>, Type> typeVarAssigns) {
        if (type == null) {
            return true;
        }

        // only a null type can be assigned to null type which
        // would have cause the previous to return true
        if (toParameterizedType == null) {
            return false;
        }

        // all types are assignable to themselves
        if (toParameterizedType.equals(type)) {
            return true;
        }

        // get the target type's raw type
        final Class<?> toClass = getRawType(toParameterizedType);
        // get the subject type's type arguments including owner type arguments
        // and supertype arguments up to and including the target class.
        final Map<TypeVariable<?>, Type> fromTypeVarAssigns = getTypeArguments(type, toClass, null);

        // null means the two types are not compatible
        if (fromTypeVarAssigns == null) {
            return false;
        }

        // compatible types, but there's no type arguments. this is equivalent
        // to comparing Map< ?, ? > to Map, and raw types are always assignable
        // to parameterized types.
        if (fromTypeVarAssigns.isEmpty()) {
            return true;
        }

        // get the target type's type arguments including owner type arguments
        final Map<TypeVariable<?>, Type> toTypeVarAssigns = getTypeArguments(toParameterizedType,
                toClass, typeVarAssigns);

        if (toTypeVarAssigns == null) {
            return false;
        }

        // now to check each type argument
        for (final TypeVariable<?> var : toTypeVarAssigns.keySet()) {
            final Type toTypeArg = unrollVariableAssignments(var, toTypeVarAssigns);
            final Type fromTypeArg = unrollVariableAssignments(var, fromTypeVarAssigns);

            if (toTypeArg == null && fromTypeArg instanceof Class) {
                continue;
            }

            // parameters must either be absent from the subject type, within
            // the bounds of the wildcard type, or be an exact match to the
            // parameters of the target type.
            if (fromTypeArg != null
                    && !fromTypeArg.equals(toTypeArg)
                    && !(toTypeArg instanceof WildcardType && isAssignable(fromTypeArg, toTypeArg,
                            typeVarAssigns))) {
                return false;
            }
        }
        return true;
    }

    private static Type unrollVariableAssignments(TypeVariable<?> var, final Map<TypeVariable<?>, Type> typeVarAssigns) {
        Type result;
        do {
            result = typeVarAssigns.get(var);
            if (result instanceof TypeVariable<?> && !result.equals(var)) {
                var = (TypeVariable<?>) result;
                continue;
            }
            break;
        } while (true);
        return result;
    }

    /**
     * <p>
     * Checks if the subject type may be implicitly cast to the target
     * generic array type following the Java generics rules.
     * </p>
     *
     * @param type the subject type to be assigned to the target type
     * @param toGenericArrayType the target generic array type
     * @param typeVarAssigns a map with type variables
     * @return {@code true} if {@code type} is assignable to
     *         {@code toGenericArrayType}.
     */
    static boolean isAssignable(final Type type, final GenericArrayType toGenericArrayType,
            final Map<TypeVariable<?>, Type> typeVarAssigns) {
        if (type == null) {
            return true;
        }

        // only a null type can be assigned to null type which
        // would have cause the previous to return true
        if (toGenericArrayType == null) {
            return false;
        }

        // all types are assignable to themselves
        if (toGenericArrayType.equals(type)) {
            return true;
        }

        final Type toComponentType = toGenericArrayType.getGenericComponentType();

        if (type instanceof Class<?>) {
            final Class<?> cls = (Class<?>) type;

            // compare the component types
            return cls.isArray()
                    && isAssignable(cls.getComponentType(), toComponentType, typeVarAssigns);
        }

        if (type instanceof GenericArrayType) {
            // compare the component types
            return isAssignable(((GenericArrayType) type).getGenericComponentType(),
                    toComponentType, typeVarAssigns);
        }

        if (type instanceof WildcardType) {
            // so long as one of the upper bounds is assignable, it's good
            for (final Type bound : getImplicitUpperBounds((WildcardType) type)) {
                if (isAssignable(bound, toGenericArrayType)) {
                    return true;
                }
            }

            return false;
        }

        if (type instanceof TypeVariable<?>) {
            // probably should remove the following logic and just return false.
            // type variables cannot specify arrays as bounds.
            for (final Type bound : getImplicitBounds((TypeVariable<?>) type)) {
                if (isAssignable(bound, toGenericArrayType)) {
                    return true;
                }
            }

            return false;
        }

        if (type instanceof ParameterizedType) {
            return false;
        }

        throw ex.illegalStateUnhandledType(type);
    }

    /**
     * <p>
     * Checks if the subject type may be implicitly cast to the target
     * wildcard type following the Java generics rules.
     * </p>
     *
     * @param type the subject type to be assigned to the target type
     * @param toWildcardType the target wildcard type
     * @param typeVarAssigns a map with type variables
     * @return {@code true} if {@code type} is assignable to
     *         {@code toWildcardType}.
     */
    private static boolean isAssignable(final Type type, final WildcardType toWildcardType,
            final Map<TypeVariable<?>, Type> typeVarAssigns) {
        // all types are assignable to themselves
        if (toWildcardType.equals(type)) {
            return true;
        }

        final Type[] toUpperBounds = getImplicitUpperBounds(toWildcardType);
        final Type[] toLowerBounds = getImplicitLowerBounds(toWildcardType);

        if (type instanceof WildcardType) {
            final WildcardType wildcardType = (WildcardType) type;
            final Type[] upperBounds = getImplicitUpperBounds(wildcardType);
            final Type[] lowerBounds = getImplicitLowerBounds(wildcardType);

            for (Type toBound : toUpperBounds) {
                // if there are assignments for unresolved type variables,
                // now's the time to substitute them.
                toBound = substituteTypeVariables(toBound, typeVarAssigns);

                // each upper bound of the subject type has to be assignable to
                // each
                // upper bound of the target type
                for (final Type bound : upperBounds) {
                    if (!isAssignable(bound, toBound, typeVarAssigns)) {
                        return false;
                    }
                }
            }

            for (Type toBound : toLowerBounds) {
                // if there are assignments for unresolved type variables,
                // now's the time to substitute them.
                toBound = substituteTypeVariables(toBound, typeVarAssigns);

                // each lower bound of the target type has to be assignable to
                // each
                // lower bound of the subject type
                for (final Type bound : lowerBounds) {
                    if (!isAssignable(toBound, bound, typeVarAssigns)) {
                        return false;
                    }
                }
            }
            return true;
        }

        for (final Type toBound : toUpperBounds) {
            // if there are assignments for unresolved type variables,
            // now's the time to substitute them.
            if (!isAssignable(type, substituteTypeVariables(toBound, typeVarAssigns),
                    typeVarAssigns)) {
                return false;
            }
        }

        for (final Type toBound : toLowerBounds) {
            // if there are assignments for unresolved type variables,
            // now's the time to substitute them.
            if (!isAssignable(substituteTypeVariables(toBound, typeVarAssigns), type,
                    typeVarAssigns)) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * Checks if the subject type may be implicitly cast to the target type
     * variable following the Java generics rules.
     * </p>
     *
     * @param type the subject type to be assigned to the target type
     * @param toTypeVariable the target type variable
     * @return {@code true} if {@code type} is assignable to
     *         {@code toTypeVariable}.
     */
    private static boolean isAssignable(final Type type, final TypeVariable<?> toTypeVariable) {
        // TODO HERE
        if (type == null) {
            return true;
        }

        // only a null type can be assigned to null type which
        // would have cause the previous to return true
        if (toTypeVariable == null) {
            return false;
        }

        // all types are assignable to themselves
        if (toTypeVariable.equals(type)) {
            return true;
        }

        if (type instanceof TypeVariable<?>) {
            // a type variable is assignable to another type variable, if
            // and only if the former is the latter, extends the latter, or
            // is otherwise a descendant of the latter.
            final Type[] bounds = getImplicitBounds((TypeVariable<?>) type);
            for (final Type bound : bounds) {
                if (isAssignable(bound, toTypeVariable)) {
                    return true;
                }
            }
        }

        if (type instanceof Class<?> || type instanceof ParameterizedType
                || type instanceof GenericArrayType || type instanceof WildcardType) {
            return false;
        }

        throw ex.illegalStateUnhandledType(type);
    }

    /**
     * <p>
     * Find the mapping for {@code type} in {@code typeVarAssigns}.
     * </p>
     *
     * @param type the type to be replaced
     * @param typeVarAssigns the map with type variables
     * @return the replaced type
     * @throws IllegalArgumentException if the type cannot be substituted
     */
    private static Type substituteTypeVariables(final Type type, final Map<TypeVariable<?>, Type> typeVarAssigns) {
        if (type instanceof TypeVariable<?> && typeVarAssigns != null) {
            final Type replacementType = typeVarAssigns.get(type);

            if (replacementType == null) {
                throw ex.illegalArgumentMissingAssignment(type);
            }
            return replacementType;
        }
        return type;
    }

    /**
     * <p>
     * Gets the type arguments of a class/interface based on a subtype. For
     * instance, this method will determine that both of the parameters for the
     * interface {@link Map} are {@link Object} for the subtype
     * {@link java.util.Properties Properties} even though the subtype does not
     * directly implement the {@code Map} interface.
     * </p>
     * <p>
     * This method returns {@code null} if {@code type} is not assignable to
     * {@code toClass}. It returns an empty map if none of the classes or
     * interfaces in its inheritance hierarchy specify any type arguments.
     * </p>
     * <p>
     * A side effect of this method is that it also retrieves the type
     * arguments for the classes and interfaces that are part of the hierarchy
     * between {@code type} and {@code toClass}. So with the above
     * example, this method will also determine that the type arguments for
     * {@link java.util.Hashtable Hashtable} are also both {@code Object}.
     * In cases where the interface specified by {@code toClass} is
     * (indirectly) implemented more than once (e.g. where {@code toClass}
     * specifies the interface {@link Iterable Iterable} and
     * {@code type} specifies a parameterized type that implements both
     * {@link Set Set} and {@link java.util.Collection Collection}),
     * this method will look at the inheritance hierarchy of only one of the
     * implementations/subclasses; the first interface encountered that isn't a
     * subinterface to one of the others in the {@code type} to
     * {@code toClass} hierarchy.
     * </p>
     *
     * @param type the type from which to determine the type parameters of
     *        {@code toClass}
     * @param toClass the class whose type parameters are to be determined based
     *        on the subtype {@code type}
     * @return a {@code Map} of the type assignments for the type variables in
     *         each type in the inheritance hierarchy from {@code type} to
     *         {@code toClass} inclusive.
     */
    static Map<TypeVariable<?>, Type> getTypeArguments(final Type type, final Class<?> toClass) {
        return getTypeArguments(type, toClass, null);
    }

    /**
     * <p>
     * Return a map of the type arguments of {@code type} in the context of {@code toClass}.
     * </p>
     *
     * @param type the type in question
     * @param toClass the class
     * @param subtypeVarAssigns a map with type variables
     * @return the {@code Map} with type arguments
     */
    private static Map<TypeVariable<?>, Type> getTypeArguments(final Type type, final Class<?> toClass,
            final Map<TypeVariable<?>, Type> subtypeVarAssigns) {
        if (type instanceof Class<?>) {
            return getTypeArguments((Class<?>) type, toClass, subtypeVarAssigns);
        }

        if (type instanceof ParameterizedType) {
            return getTypeArguments((ParameterizedType) type, toClass, subtypeVarAssigns);
        }

        // TODO HERE
        if (type instanceof GenericArrayType) {
            return getTypeArguments(((GenericArrayType) type).getGenericComponentType(), toClass
                    .isArray() ? toClass.getComponentType() : toClass, subtypeVarAssigns);
        }

        // since wildcard types are not assignable to classes, should this just
        // return null?
        if (type instanceof WildcardType) {
            for (final Type bound : getImplicitUpperBounds((WildcardType) type)) {
                // find the first bound that is assignable to the target class
                if (isAssignable(bound, toClass)) {
                    return getTypeArguments(bound, toClass, subtypeVarAssigns);
                }
            }

            return null;
        }

        if (type instanceof TypeVariable<?>) {
            for (final Type bound : getImplicitBounds((TypeVariable<?>) type)) {
                // find the first bound that is assignable to the target class
                if (isAssignable(bound, toClass)) {
                    return getTypeArguments(bound, toClass, subtypeVarAssigns);
                }
            }

            return null;
        }
        throw ex.illegalStateUnhandledType(type);
    }

    /**
     * <p>
     * Return a map of the type arguments of a parameterized type in the context of {@code toClass}.
     * </p>
     *
     * @param parameterizedType the parameterized type
     * @param toClass the class
     * @param subtypeVarAssigns a map with type variables
     * @return the {@code Map} with type arguments
     */
    private static Map<TypeVariable<?>, Type> getTypeArguments(
            final ParameterizedType parameterizedType, final Class<?> toClass,
            final Map<TypeVariable<?>, Type> subtypeVarAssigns) {
        final Class<?> cls = getRawType(parameterizedType);

        // make sure they're assignable
        if (!isAssignable(cls, toClass)) {
            return null;
        }

        if (cls == null) {
            throw new IllegalArgumentException("Invalid parameterized type: " + parameterizedType);
        }

        final Type ownerType = parameterizedType.getOwnerType();
        Map<TypeVariable<?>, Type> typeVarAssigns;

        if (ownerType instanceof ParameterizedType) {
            // get the owner type arguments first
            final ParameterizedType parameterizedOwnerType = (ParameterizedType) ownerType;
            typeVarAssigns = getTypeArguments(parameterizedOwnerType,
                    getRawType(parameterizedOwnerType), subtypeVarAssigns);
        } else {
            // no owner, prep the type variable assignments map
            typeVarAssigns = subtypeVarAssigns == null ? new HashMap<>()
                    : new HashMap<>(subtypeVarAssigns);
        }

        // get the subject parameterized type's arguments
        final Type[] typeArgs = parameterizedType.getActualTypeArguments();
        // and get the corresponding type variables from the raw class
        final TypeVariable<?>[] typeParams = cls.getTypeParameters();

        // map the arguments to their respective type variables
        for (int i = 0; i < typeParams.length; i++) {
            final Type typeArg = typeArgs[i];
            typeVarAssigns.put(typeParams[i], typeVarAssigns.getOrDefault(typeArg, typeArg));
        }

        if (toClass.equals(cls)) {
            // target class has been reached. Done.
            return typeVarAssigns;
        }

        // walk the inheritance hierarchy until the target class is reached
        return getTypeArguments(getClosestParentType(cls, toClass), toClass, typeVarAssigns);
    }

    /**
     * <p>
     * Return a map of the type arguments of a class in the context of {@code toClass}.
     * </p>
     *
     * @param cls the class in question
     * @param toClass the context class
     * @param subtypeVarAssigns a map with type variables
     * @return the {@code Map} with type arguments
     */
    private static Map<TypeVariable<?>, Type> getTypeArguments(Class<?> cls, final Class<?> toClass,
            final Map<TypeVariable<?>, Type> subtypeVarAssigns) {

        if (toClass == null) {
            throw new IllegalArgumentException("`toClass` must not be `null`");
        }

        // make sure they're assignable
        if (!isAssignable(cls, toClass)) {
            return null;
        }

        // can't work with primitives
        if (cls != null && cls.isPrimitive()) {
            // both classes are primitives?
            if (toClass.isPrimitive()) {
                // dealing with widening here. No type arguments to be
                // harvested with these two types.
                return new HashMap<>();
            }

            // work with wrapper the wrapper class instead of the primitive
            cls = ClassUtils.primitiveToWrapper(cls);
        }

        // create a copy of the incoming map, or an empty one if it's null
        final HashMap<TypeVariable<?>, Type> typeVarAssigns = subtypeVarAssigns == null ? new HashMap<>()
                : new HashMap<>(subtypeVarAssigns);

        // has target class been reached?
        if (toClass.equals(cls)) {
            return typeVarAssigns;
        }

        // walk the inheritance hierarchy until the target class is reached
        return getTypeArguments(getClosestParentType(cls, toClass), toClass, typeVarAssigns);
    }

    /**
     * <p>
     * Get the closest parent type to the
     * super class specified by {@code superClass}.
     * </p>
     *
     * @param cls the class in question
     * @param superClass the super class
     * @return the closes parent type
     */
    private static Type getClosestParentType(final Class<?> cls, final Class<?> superClass) {
        // only look at the interfaces if the super class is also an interface
        if (superClass.isInterface()) {
            // get the generic interfaces of the subject class
            final Type[] interfaceTypes = cls.getGenericInterfaces();
            // will hold the best generic interface match found
            Type genericInterface = null;

            // find the interface closest to the super class
            for (final Type midType : interfaceTypes) {
                Class<?> midClass;

                if (midType instanceof ParameterizedType) {
                    midClass = getRawType((ParameterizedType) midType);
                } else if (midType instanceof Class<?>) {
                    midClass = (Class<?>) midType;
                } else {
                    throw ex.illegalStateUnexpectedGenericInterface(midType);
                }

                // check if this interface is further up the inheritance chain
                // than the previously found match
                if (isAssignable(midClass, superClass)
                        && isAssignable(genericInterface, (Type) midClass)) {
                    genericInterface = midType;
                }
            }

            // found a match?
            if (genericInterface != null) {
                return genericInterface;
            }
        }

        // none of the interfaces were descendants of the target class, so the
        // super class has to be one, instead
        return cls.getGenericSuperclass();
    }

    /**
     * <p>
     * This method strips out the redundant upper bound types in type
     * variable types and wildcard types (or it would with wildcard types if
     * multiple upper bounds were allowed).
     * </p>
     * <p>
     * Example, with the variable
     * type declaration:
     *
     * <pre>
     * &lt;K extends java.util.Collection&lt;String&gt; &amp;
     * java.util.List&lt;String&gt;&gt;
     * </pre>
     *
     * <p>
     * since {@code List} is a subinterface of {@code Collection},
     * this method will return the bounds as if the declaration had been:
     * </p>
     *
     * <pre>
     * &lt;K extends java.util.List&lt;String&gt;&gt;
     * </pre>
     *
     * @param bounds an array of types representing the upper bounds of either
     *        {@link WildcardType} or {@link TypeVariable}, not {@code null}.
     * @return an array containing the values from {@code bounds} minus the
     *         redundant types.
     */
    public static Type[] normalizeUpperBounds(final Type[] bounds) {
        // don't bother if there's only one (or none) type
        if (bounds.length < 2) {
            return bounds;
        }

        final Set<Type> types = new HashSet<>(bounds.length);

        for (final Type type1 : bounds) {
            boolean subtypeFound = false;

            for (final Type type2 : bounds) {
                if (type1 != type2 && isAssignable(type2, type1, null)) {
                    subtypeFound = true;
                    break;
                }
            }

            if (!subtypeFound) {
                types.add(type1);
            }
        }

        return types.toArray(new Type[0]);
    }

    /**
     * <p>
     * Returns an array containing the sole type of {@link Object} if
     * {@link TypeVariable#getBounds()} returns an empty array. Otherwise, it
     * returns the result of {@link TypeVariable#getBounds()} passed into
     * {@link #normalizeUpperBounds}.
     * </p>
     *
     * @param typeVariable the subject type variable, not {@code null}
     * @return a non-empty array containing the bounds of the type variable.
     */
    private static Type[] getImplicitBounds(final TypeVariable<?> typeVariable) {
        final Type[] bounds = typeVariable.getBounds();
        return bounds.length == 0 ? new Type[] { Object.class } : normalizeUpperBounds(bounds);
    }

    /**
     * <p>
     * Returns an array containing the sole value of {@link Object} if
     * {@link WildcardType#getUpperBounds()} returns an empty array. Otherwise,
     * it returns the result of {@link WildcardType#getUpperBounds()}
     * passed into {@link #normalizeUpperBounds}.
     * </p>
     *
     * @param wildcardType the subject wildcard type, not {@code null}
     * @return a non-empty array containing the upper bounds of the wildcard
     *         type.
     */
    static Type[] getImplicitUpperBounds(final WildcardType wildcardType) {
        final Type[] bounds = wildcardType.getUpperBounds();
        return bounds.length == 0 ? new Type[] { Object.class } : normalizeUpperBounds(bounds);
    }

    /**
     * <p>
     * Returns an array containing a single value of {@code null} if
     * {@link WildcardType#getLowerBounds()} returns an empty array. Otherwise,
     * it returns the result of {@link WildcardType#getLowerBounds()}.
     * </p>
     *
     * @param wildcardType the subject wildcard type, not {@code null}
     * @return a non-empty array containing the lower bounds of the wildcard
     *         type.
     */
    static Type[] getImplicitLowerBounds(final WildcardType wildcardType) {
        final Type[] bounds = wildcardType.getLowerBounds();
        return bounds.length == 0 ? new Type[] { null } : bounds;
    }

    /**
     * <p>
     * Transforms the passed in type to a {@link Class} object. Type-checking method of convenience.
     * </p>
     *
     * @param parameterizedType the type to be converted
     * @return the corresponding {@code Class} object
     * @throws IllegalStateException if the conversion fails
     */
    private static Class<?> getRawType(final ParameterizedType parameterizedType) {
        final Type rawType = parameterizedType.getRawType();
        return (Class<?>) rawType;
    }

    /**
     * Returns the raw type if the given type is parameterized, if not returns the type.
     *
     * @param type the type to convert
     * @return the raw type if parameterized
     */
    public static Type getRawTypeIfParameterized(Type type) {
        if (type instanceof ParameterizedType) {
            return ((ParameterizedType) type).getRawType();
        }
        return type;
    }

    /**
     * Check equality of types.
     *
     * @param t1 the first type
     * @param t2 the second type
     * @return boolean
     */
    public static boolean equals(final Type t1, final Type t2) {
        if (Objects.equals(t1, t2)) {
            return true;
        }
        if (t1 instanceof ParameterizedType) {
            return equals((ParameterizedType) t1, t2);
        }
        if (t1 instanceof GenericArrayType) {
            return equals((GenericArrayType) t1, t2);
        }
        if (t1 instanceof WildcardType) {
            return equals((WildcardType) t1, t2);
        }
        return false;
    }

    private static boolean equals(final ParameterizedType p, final Type t) {
        if (t instanceof ParameterizedType) {
            final ParameterizedType other = (ParameterizedType) t;
            if (equals(p.getRawType(), other.getRawType()) && equals(p.getOwnerType(), other.getOwnerType())) {
                return equals(p.getActualTypeArguments(), other.getActualTypeArguments());
            }
        }
        return false;
    }

    private static boolean equals(final GenericArrayType a, final Type t) {
        return t instanceof GenericArrayType
                && equals(a.getGenericComponentType(), ((GenericArrayType) t).getGenericComponentType());
    }

    private static boolean equals(final WildcardType w, final Type t) {
        if (t instanceof WildcardType) {
            final WildcardType other = (WildcardType) t;
            return equals(getImplicitLowerBounds(w), getImplicitLowerBounds(other))
                    && equals(getImplicitUpperBounds(w), getImplicitUpperBounds(other));
        }
        return false;
    }

    private static boolean equals(final Type[] t1, final Type[] t2) {
        if (t1.length == t2.length) {
            for (int i = 0; i < t1.length; i++) {
                if (!equals(t1[i], t2[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}
