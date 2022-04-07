package com.zinkworks.connectors.demo;

import static org.assertj.core.api.BDDAssertions.then;

import org.junit.jupiter.api.Test;

class AppTest {

  @Test
  void shouldHaveAGreeting() {
    // given
    final App classUnderTest = new App();

    // when
    final String result = classUnderTest.getGreeting();

    // then
    then(result)
        .as("app should have a greeting")
        .isNotNull();
  }
}
