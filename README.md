# Distributed Map-Reduce System for Inverted Index and Word Count

![Distributed Map-Reduce System]image.webp

## Overview
This project implements a distributed map-reduce system in Python, designed to efficiently process large datasets for creating inverted indexes and performing word count operations. The system leverages the principles of parallel processing, distributing the workload across multiple nodes to speed up the data processing tasks.

## Features
- **Distributed Processing:** Utilizes multiple processing nodes to divide and conquer large datasets, ensuring efficient data handling and processing speed.
- **Inverted Index Generation:** Builds an inverted index that maps words to their locations in a set of documents, facilitating quick full-text searches.
- **Word Count:** Efficiently counts the occurrences of each word in the dataset, providing essential insights into data analytics.
- **Scalability:** Designed to scale horizontally, allowing the addition of more nodes to the system to handle larger datasets without a significant increase in processing time.
- **Fault Tolerance:** Implements strategies to handle failures, ensuring the system continues to operate effectively in the event of node failures.

## Getting Started
### Prerequisites
- Python 3.x
- Additional Python libraries: [List any libraries or frameworks used]

### Installation
1. Clone the repository to your local machine:
2. Navigate to the `Map reduce` directory:
3. Install the required Python packages:


### Running the System
To start processing your data with the map-reduce system, follow these steps:
1. run command - python store.py - to start custom key value store
2. run command - python master.py after altering the config file

