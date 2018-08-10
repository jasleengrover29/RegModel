# RegModel
This repository contains Map Reduce code for Hadoop to build a mutiple regression model. This model helps determine the relationship between two predictor variables and one response variable . The code is written in JAVA. 

The input data should be comma separated and each record must contain three values in order: One response variable (y), first predictor variable(x1), second predictor variable (x2). Mapper reads input file line by line. To avoid shuffling of values in record, I have passed a string of each row (which contains these three values separated by a delimiter) from mapper to reducer. 
The reduce function in the reducer reads the output from mapper, breaks each record based on the delimiter, converts it into a numeric value and stores the values of y, x1 and x2 in three separate array lists. By putting these values in various formulas, beta coefficients are computed. Once we the equation of the best fitted line, a new value of y is predicted by putting the values of x1 & x2 into the equation. Error in each value is recorded on calculating the difference between the predicted value and the original value. By doing so we can also find out R squared value by applying a fomula. 
 
The regression equation and the R squared value is determined mainly by mathematical formulas and no pre defined package of library is used to find beta coefficients.
Finally beta coefficients and the R squared value is written into the output file directory.
