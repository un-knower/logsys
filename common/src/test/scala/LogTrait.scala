import org.slf4j.LoggerFactory

/**
 * Created by fj on 16/10/29.
 */
trait LogTrait {

    def LOG = LoggerFactory.getLogger(this.getClass)

}
