package io.smallrye.reactive.messaging.jms;

import java.util.Enumeration;

/**
 * Structure handling JMS Message properties.
 * Instances of this interface must be immutable.
 */
public interface JmsProperties {

    /**
     * Creates a builder object to create JMS Properties
     *
     * @return the builder.
     */
    static JmsPropertiesBuilder builder() {
        return new JmsPropertiesBuilder();
    }

    /**
     * Indicates whether a property value exists.
     *
     * @param name the name of the property to test
     * @return true if the property exists
     */
    boolean propertyExists(String name);

    /**
     * Returns the value of the {@code boolean} property with the specified name.
     *
     * @param name the name of the {@code boolean} property
     * @return the {@code boolean} property value for the specified name
     */
    boolean getBooleanProperty(String name);

    /**
     * Returns the value of the {@code byte} property with the specified name.
     *
     * @param name the name of the {@code byte} property
     * @return the {@code byte} property value for the specified name
     */
    byte getByteProperty(String name);

    /**
     * Returns the value of the {@code short} property with the specified name.
     *
     * @param name the name of the {@code short} property
     * @return the {@code short} property value for the specified name
     */
    short getShortProperty(String name);

    /**
     * Returns the value of the {@code int} property with the specified name.
     *
     * @param name the name of the {@code int} property
     * @return the {@code int} property value for the specified name
     */
    int getIntProperty(String name);

    /**
     * Returns the value of the {@code long} property with the specified name.
     *
     * @param name the name of the {@code long} property
     * @return the {@code long} property value for the specified name
     */
    long getLongProperty(String name);

    /**
     * Returns the value of the {@code float} property with the specified name.
     *
     * @param name the name of the {@code float} property
     * @return the {@code float} property value for the specified name
     */
    float getFloatProperty(String name);

    /**
     * Returns the value of the {@code double} property with the specified name.
     *
     * @param name the name of the {@code double} property
     * @return the {@code double} property value for the specified name
     */
    double getDoubleProperty(String name);

    /**
     * Returns the value of the {@code String} property with the specified name.
     *
     * @param name the name of the {@code String} property
     * @return the {@code String} property value for the specified name; if there is no property by this name, a null value
     *         is returned
     */
    String getStringProperty(String name);

    /**
     * Returns the value of the Java object property with the specified name.
     *
     * <p>
     * This method can be used to return, in objectified format, an object that has been stored as a property in the message
     * with the equivalent <code>setObjectProperty</code> method call, or its equivalent primitive
     * <code>set<I>type</I>Property</code> method.
     *
     * @param name the name of the Java object property
     * @return the Java object property value with the specified name, in objectified format (for example, if the property
     *         was set as an {@code int}, an {@code Integer} is returned); if there is no property by this name, a null value is
     *         returned
     */
    Object getObjectProperty(String name);

    /**
     * Returns an {@code Enumeration} of all the property names.
     *
     * <p>
     * Note that Jakarta Messaging standard header fields are not considered properties and are not returned in this
     * enumeration.
     *
     * @return an enumeration of all the names of property values
     */
    Enumeration<String> getPropertyNames();

}
