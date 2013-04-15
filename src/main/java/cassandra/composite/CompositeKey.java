package cassandra.composite;

import com.netflix.astyanax.annotations.Component;

/**
 * @author Gyozo_Nyari
 *
 */
public class CompositeKey {

    @Component(ordinal = 0)
    public long fileId;
    @Component(ordinal = 1)
    public long trackId;
    @Component(ordinal = 2)
    public String field;

    public CompositeKey() {
    }

}
