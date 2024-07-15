Example repository to reproduce issue https://github.com/testcontainers/testcontainers-java/issues/8867

Execute the tests using: `gradlew build`

It's expected that the test fails. But it shouldn't take 2 minutes
to shutdown the test after it ran.