package tz.co.juutech.extractor.exception;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/8/21.
 */
public class InvalidMandatoryPropertyValueException extends Exception {
    public InvalidMandatoryPropertyValueException(String propertyName, String propertyValue) {
        super(String.format("Mandatory property %s value (%s) not valid", propertyName, propertyValue));
    }
}
