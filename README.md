# Distributed Map-Reduce Django Application for Inverted Index and Word Count

![Distributed Map-Reduce System](/distributed_map_reduce/image.webp)


## Overview
This project integrates a Django web application with a distributed map-reduce backend, demonstrating the power of combining Django's web capabilities with the map-reduce model for scalable data processing.
## Features
- **Distributed Processing:** Utilizes multiple processing nodes to divide and conquer large datasets, ensuring efficient data handling and processing speed.
- **Inverted Index Generation:** Builds an inverted index that maps words to their locations in a set of documents, facilitating quick full-text searches.
- **Word Count:** Efficiently counts the occurrences of each word in the dataset, providing essential insights into data analytics.
- **Scalability:** Designed to scale horizontally, allowing the addition of more nodes to the system to handle larger datasets without a significant increase in processing time.
- **Fault Tolerance:** Implements strategies to handle failures, ensuring the system continues to operate effectively in the event of node failures.

## Getting Started
### Prerequisites
- Python 3.8 and above

### Installation
1. Clone the repository to your local machine:
2. Navigate to the `Map reduce` directory:
3. cd Map_reduce
    pip install -r distributed_map_reduce/requirements.txt
4. Apply migrations and start django server
   python manage.py migrate
   python manage.py runserver


## Usage
After starting the server, visit http://127.0.0.1:8000/config/ to configure and initiate map-reduce tasks like word_count or inverted_index.
Enter the text in the input box, Output shows the output of map reduce process


## Features
Configurable map-reduce settings via a web interface.
Supports multiple map-reduce algorithms.

