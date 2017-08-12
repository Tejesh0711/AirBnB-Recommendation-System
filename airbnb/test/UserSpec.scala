import play.api.test.{FakeRequest, WithApplication}

/**
  * Created by vinay on 4/26/17.
  */
class UserSpec extends  Specification {


  "Application" should {


    "should create the User" in new WithApplication{
      val home = route(FakeRequest(POST, "/signUp")).get


      status(home) must equalTo(OK)
      contentType(home) must beSome.which(_ == "text/html")
    }
  }

}
