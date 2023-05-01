# Introduction

One of the most popular and effective enterprise case-studies which leverage analytics today is log analytics. 
Almost every small and big organization today have multiple systems and infrastructure running day in and day out. 
To effectively keep their business running, organizations need to know if their infrastructure is performing to its maximum potential. 
This involves analyzing system and application logs and maybe even apply predictive analytics on log data. 
The amount of log data is typically massive, depending on the type of organizational infrastructure and applications running on it. 
Gone are the days when we were limited by just trying to analyze a sample of data on a single machine due to compute constraints.
big data processing and open-source analytics frameworks like Spark, we can perform scalable log analytics on 
potentially millions and billions of log messages daily. The intent of this case-study oriented tutorial is to take a 
hands-on approach to showcasing how we can leverage Spark to perform log analytics at scale on semi-structured log data.

Like we mentioned before, Apache Spark is an excellent and ideal open-source framework for wrangling, 
analyzing and modeling on structured and unstructured data. Typically, server logs are a very common data source in 
enterprises and often contain a gold mine of actionable insights and information. 
Log data comes from many sources in an enterprise, such as the web, client and compute servers, 
applications, user-generated content, flat files. They can be used for monitoring servers, improving business and 
customer intelligence, building recommendation systems, fraud detection, and much more.

Spark allows you to dump and store your logs in files on disk cheaply, while still providing rich APIs to perform data analysis at scale.
In this project we will analyze log datasets from NASA Kennedy Space Center web server in Florida.

We have 2 datasets that contain two months’ worth of all HTTP requests to the NASA Kennedy Space Center WWW server in Florida.