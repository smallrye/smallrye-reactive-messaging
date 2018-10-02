# Implementation of the MicroProfile Reactive Messaging specification


## How to build


You need to build the SmallRye Reactive Stream Operator implementation first:

```bash
git clone git@github.com:smallrye/smallrye-reactive-streams-operators.git
cd smallrye-reactive-streams-operators
mvn clean install
```

Then, build this repository

```bash
cd $ROOT
mvn clean install
```

## How to contribute

Just open a pull request. Makes sure to run the tests and the TCK before opening the PR. Don't forget that documentation 
and tests are as important as the code (if not more). 
