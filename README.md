# 6.824 Distributed Systems

## Course Resources
Notes: https://learncs.me/mit/6.824  
Lectures: https://www.youtube.com/playlist?list=PLrw6a1wE39_tb2fErI4-WkMbsvGQk9_UB

## Running Tests
To run a specific test, use the following command in your terminal. You can replace the `2A` with the test name you wish to run:

```script
go test -run 2A // or go test -run TestFailNoAgree2B
```

</br>  

Check for Race Conditions
```
go test -race -run TestFailNoAgree2B
```