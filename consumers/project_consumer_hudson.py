"""
project_consumer_hudson.py

Consume JSON messages from a Kafka topic and visualize average message length per category in real-time.

JSON is a set of key:value pairs.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting occurrences

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Import Matplotlib.pyplot for live plotting
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "buzzline-topic")

def get_kafka_server() -> str:
    return os.getenv("KAFKA_SERVER", "localhost:9092")

##########################################
# Calculate average message length per category
##########################################

def calculate_avg_message_length(messages):
    avg_lengths = {category: sum(lengths) / len(lengths) for category, lengths in messages.items()}
    return avg_lengths

##########################################
# Set up data structures for live plotting
##########################################

category_lengths = defaultdict(list)
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

# Create a tuple containing a figure and an axis
fig, ax = plt.subplots()

# Turn on interactive mode for live updates
plt.ion()

############################################################
# Define an update chart function for live plotting
############################################################

def update_chart():
    """Update the live chart with the latest data."""
    # Clear the previous chart
    ax.clear()

    # Get the categories and average message lengths
    categories = list(avg_message_lengths.keys())
    avg_lengths = list(avg_message_lengths.values())
    colors = [CATEGORY_COLORS.get(category, "gray") for category in categories]

    # Create a bar chart
    ax.bar(categories, avg_lengths, color=colors)

    # Set the labels and title
    ax.set_xlabel("Categories")
    ax.set_ylabel("Average Message Length")
    ax.set_title("Real-Time Average Message Length per Category")

    # Rotate the x-axis labels
    ax.set_xticklabels(categories, rotation=45, ha="right")

    # Adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow time for the chart to render
    plt.pause(0.01)

######################################
# Function to process a single message
######################################

def process_message(message):
    try:
        # Decode the message from bytes to string
        message = message.decode('utf-8')

        # Parse the JSON string into a dictionary
        message_dict = json.loads(message)

        # Print the message for debugging purposes
        print(f"Decoded message: {message_dict}")
        
        # Extract category and message_length
        category = message_dict.get("category", "other")
        message_length = message_dict.get("message_length", 0)

        # Append the message length to the respective category
        category_lengths[category].append(message_length)

        # Calculate average message lengths
        global avg_message_lengths
        avg_message_lengths = calculate_avg_message_length(category_lengths)
        logger.info(f"Average message length per category: {avg_message_lengths}")
        
        # Update the live chart
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
        value_deserializer=lambda x: x  # Do not decode message value
    )

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
    main()

    # Turn off interactive mode after completion
    plt.ioff()  

    # Display the final chart
    plt.show()
