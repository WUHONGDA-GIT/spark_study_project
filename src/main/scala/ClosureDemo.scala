class ClosureDemo {
  def func() = {
    val user = new User(3)
    val add: Int => Int = (i: Int) => i + user.age
    add
  }

}



