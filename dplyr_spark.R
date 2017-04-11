# Code in support of Nursing School Lecture - 04.12.17 Pittard wsp@emory.edu
filter(df, id %in% c(1,3,5))
filter(df, id %in% seq(1,nrow(df),2))


mutate(df,meanage = mean(age))
mutate(df, age = age/10)

mutate(df,old_young=ifelse(df$age>=mean(df$age),"Y","N"))

# Let's plot everyone and if their respective age is above
# the mean average for all then we'll give them a color of red
# if not then blue

tmp <- mutate(df, color = ifelse(age > mean(age),"red","blue"))

ggplot(tmp,aes(x=id,y=age)) +geom_point(aes(color=color),size=3) +
  geom_hline(aes(yintercept=mean(age))) +
  labs(title="Age per Person",color="Mean Age\n") +
  scale_color_manual(labels = c("Below", "Above"), 
                     values = c("blue", "red")) 

#

arrange(df, desc(age))
arrange(df, gender,desc(age))


select(df,gender,id,age)  # Reorder the columns
select(df,-age)   # Select all but the age column
select(df,id:age)  # Can use : to select a range

library(ggplot2)
data(diamonds)
names(diamonds)
nrow(diamonds)

select(diamonds, starts_with("c"))
select(diamonds,ends_with("t"))


testdf <- expand.grid(m_1=seq(60,70,10),age=c(25,32),m_2=seq(50,60,10))
head(testdf, 4)

select(testdf,matches("_"))

select(testdf,num_range("m_",1:2))


# See https://blog.rstudio.org/2016/09/27/sparklyr-r-interface-for-apache-spark/

require(sparklyr)
sc <- spark_connect(master = "local")

# 
# The following data sets are built into Base R and dplyr
# These statements will copy these R dataframes into the Spark Cluster
#

require(dplyr)
iris_tbl <- copy_to(sc, iris)
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")
batting_tbl <- copy_to(sc, Lahman::Batting, "batting")

#

# filter by departure delay
flights_tbl %>% filter(dep_delay == 2)

# The following will help us see patterns for flight delays in the nyc flight data
# We split the complete dataset into individual planes and then summarise each plane 
# by counting the number of flights (count = n()) and computing the average distance 
# (dist = mean(Distance, na.rm = TRUE)) and arrival delay 
# (delay = mean(ArrDelay, na.rm = TRUE)). We then use ggplot2 to display the output.

delay <- flights_tbl %>% 
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect()

# plot delays

require(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)


## MLIB


# copy mtcars into spark

mtcars_tbl <- copy_to(sc, mtcars)

# transform our data set, and then partition into 'training', 'test'
partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)

# fit a linear model to the training dataset - this uses the Spark Library

fit <- partitions$training %>%
  ml_linear_regression(response = "mpg", features = c("wt", "cyl"))

summary(fit)


# It is possible to use real SQL to access a Spark Dataframe
# although it isn't necessary since dplyr provides a way to do so

require(DBI)
iris_preview <- dbGetQuery(sc, "SELECT * FROM iris LIMIT 10")

iris_preview




