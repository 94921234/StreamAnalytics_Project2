# Reflection and Discussion Summary

## Project Reflection

The project involved gaining hands-on knowledge about building a full-stack real-time data pipeline that includes ingestion, processing, storing, and visualizing the data. In addition to working with real time data through Event Hub’s and Spark's Structured Streaming we applied several important concepts in our solution such as: windowing; aggregation; stream-steam joins; and watermarking when dealing with late arriving data.

One of the main things I learned is that in order to build a true real-time system you need to have tight integration across all components. Any small inconsistencies in schema, timing or configuration can cause an entire pipeline to fail. Therefore it is very important that you are able to coordinate all of your components from start to finish. Another thing I realized was the importance of partitioning (by `zone_id`), to achieve both scalability and proper distributed processing.


## Team Collaboration

The application development process used an architecture that separated out the different levels of responsibility as follows:

- Ingestion (Event Hubs + producer)  
- Processing (Spark)  
- Storage (Azure Blob)  
- Visualization (dashboard)  

Although the team has established a solid separation of responsibilities for each layer of their application, they still had to collaborate continuously on:

- Ensuring consistent formatting and schema for the data being sent through each component
- Guaranteeing a smooth flow of data through each of the separate components
- Troubleshooting integration problems with each other

Through iterative work and testing, the team refined both the pipeline of how the data would move through the system and the uses for analytics.


## Insights on Use Case Selection & Real-World Applicability

The use case(s) selected for this project were determined through an iterative collaboration. Our focus was on developing data analytic tools that could be utilized within a streaming data architecture as well as represent common real world issues associated with ride hailing and delivery services.

Use Cases UC1, UC2, and UC3 were chosen due to their ability to create a sequential progression of analytical tool development from basic to complex.

- Basic Demand Tracking 
- Demand/Supply Analysis 
- Actionable Insights such as Surge Detection

These use cases were inspired by how platforms like Uber Eats, Glovo, and Deliveroo operate:

- **UC1 (Demand monitoring):** Identifying high-demand zones  
- **UC2 (Demand vs supply):** Balancing couriers and orders and detecting **courier idle inefficiency**  
- **UC3 (Surge detection):** Responding to imbalances and identifying **pricing anomalies** compared to historical trends or other zones  

Overall, the selected use cases support:

- Resource allocation  
- Operational efficiency  
- Dynamic pricing decisions  


## Analytics Implementation (Dashboard)

- **UC1:** Bar chart of orders per zone per minute → identifies busiest zones  
- **UC2:** Demand vs supply comparison → highlights imbalance and **idle couriers**  
- **UC3:** Surge detection table and map → flags zones in surge and reveals **real-time pricing anomalies**  


## Challenges Encountered

- The ability to process a large number of simultaneous, real-time feeds 
- Developing an efficient method for executing time-based join operations 
- Troubleshooting distributed Spark streaming applications 
- Guaranteeing reliability of all components from beginning to end in the end-to-end pipeline

## Last Thoughts

Real-time analytics can be much more than just simply processing information. Real-time analytics can provide valuable, actionable insights as soon as they occur. This system has shown this concept by demonstrating how it applies to real world environments with the need for speed and efficiency.
