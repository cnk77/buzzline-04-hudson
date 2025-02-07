"""
project_consumer_hudson.py

Consume json messages from a Kafka topic and visualize average message length per category in real-time.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{'message': 'I just found travel! It was funny.', 'author': 'Eve', 'timestamp': '2025-02-06 14:22:28',
 'category': 'travel', 'sentiment': 0.85, 'keyword_mentioned': 'travel', 'message_length': 34}"

Example JSON message (after deserialization) to be analyzed
{'category': 'travel', 'message_length': 34}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaConsumer

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################
### Note: this is different than example, review if getting errors###

def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "buzzline-topic")

def get_kafka_server() -> str:
    return os.getenv("KAFKA_SERVER", "localhost:9092")

################################################
# Calculate average message length per category
################################################

def calculate_avg_message_length(messages):
    category_lengths = defaultdict(list)
    
    for message in messages:
        category = message['category']
        message_length = message['message_length']
        category_lengths[category].append(message_length)
    
    avg_lengths = {category: sum(lengths) / len(lengths) for category, lengths in category_lengths.items()}
    return avg_lengths

##########################################
# Set up data structures for live plotting
##########################################

category_counts = defaultdict(int)
avg_message_lengths = {}

##################################
# Define colors for each category
##################################

CATEGORY_COLORS = {
    "humor": "salmon",
    "tech": "cadetblue",
    "food": "mediumseagreen",
    "travel": "darkgoldenrod",
    "entertainment": "gold",
    "gaming": "thistle",
    "other": "lavender"
}

#####################
# Set up live visuals
#####################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

############################################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
############################################################


def update_chart():
    """Update the live chart with the latest author counts."""
    # Clear the previous chart
    ax.clear()

    # Get the categoris and average message lengths from the dictionary
    categories = list(avg_message_lengths.keys())
    avg_lengths = list(avg_message_lengths.values())
    colors = [CATEGORY_COLORS.get(category, "gray") for category in categories]

    # Create a bar chart using the bar() method.
    # Pass in the x list, the y list, and the color
    ax.bar(categories, avg_lengths, color=colors)

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Categories")
    ax.set_ylabel("Average Message Length")
    ax.set_title("Real-Time Average Message Length per Category")

    # Use the set_xticklabels() method to rotate the x-axis labels
    # Pass in the x list, specify the rotation angle is 45 degrees,
    # and align them to the right
    # ha stands for horizontal alignment
    ax.set_xticklabels(categories, rotation=45, ha="right")

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)

######################################
# Function to process a single message
# ####################################

def process_message(message):
    try:
        message_dict = json.loads(message)
        category = message_dict.get("category", "other")
        message_length = message_dict.get("message_length", 0)
        
        category_lengths[category].append(message_length)
        
        avg_message_lengths.update(calculate_avg_message_length(category_lengths))
        logger.info(f"Average message length per category: {avg_message_lengths}")
        update_chart()
        
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


######################################
# Define main function for this module
######################################


def main():
    logger.info("START consumer...")
    topic = get_kafka_topic()
    kafka_server = get_kafka_server()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    messages = []

    try:
        for message in consumer:
            process_message(message.value)
            
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":

    # Call the main function to start the consumer
    main()

    # Turn off interactive mode after completion
    plt.ioff()  

    # Display the final chart
    plt.show()
