package cassandra;

import java.util.Map;

/**
 * Counts authentication failures.
 * @author Gyozo_Nyari
 *
 */
public interface CountingDao {

	/**
	 * Returns the number of authentication failures for given user's credentials
	 * @param userId user id
	 * @param credentials credentials
	 * @return number of authentication failures
	 */
    int getNumberOfAuthenticationFailures(final String userId, final String credentials);

    /**
     * Returns the number of authentication failures for given user's
     * @param userId user id
     * @return map of credentials and auth failures
     */
    Map<String, Integer> getNumberOfAuthenticationFailures(final String userId);

    /**
     * Sets the number of authentication failures
     * @param userId user id
     * @param credentials credentials
     * @param value to be set
     */
    void setNumberOfAuthenticationFailures(final String userId, final String credentials, final int value);

    /**
     * Increments the number of authentication failures.
     * @param userId user id
     * @param credentials credentials
     */
    void incrementNumberOfAuthenticationFailures(final String userId, final String credentials);

}
