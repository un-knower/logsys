/**
 * Created by fj on 16/10/29.
 */
class ScalaTest {

    trait User {
        def name: String=""
    }

    trait DB {
        def name: String
    }

    trait B {
        //self-type
        user: User =>
        def foo {
            println(this.name)
        }

        def ok: Unit ={

        }
    }

    object Main extends B with User {
        //override def name: String = "sds"
        this.ok
    }

}
