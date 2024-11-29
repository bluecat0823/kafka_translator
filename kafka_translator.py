# File: kafka_translator.py

import json
from kafka import KafkaProducer, KafkaConsumer
from googletrans import Translator  # Install with: pip install googletrans==4.0.0-rc1

# Kafka Configurations
BROKER = 'localhost:9092'  # Replace with your Kafka broker
RAW_TEXT_TOPIC = 'raw_text'
TRANSLATED_TEXT_TOPIC = 'translated_text'

# Initialize Google Translator
translator = Translator()

# Kafka Producer
def produce_raw_text():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Enter text to translate (type 'exit' to quit):")
    while True:
        user_input = input("Text: ")
        if user_input.lower() == 'exit':
            break
        target_lang = input("Target Language (e.g., 'es' for Spanish, 'fr' for French): ")
        data = {"text": user_input, "target_lang": target_lang}
        producer.send(RAW_TEXT_TOPIC, data)
        print(f"Sent to Kafka: {data}")
    producer.close()

# Kafka Consumer for Translation
def consume_and_translate():
    consumer = KafkaConsumer(
        RAW_TEXT_TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Listening for text to translate...")
    for message in consumer:
        data = message.value
        text = data.get("text")
        target_lang = data.get("target_lang")
        try:
            translated = translator.translate(text, dest=target_lang)
            result = {
                "original_text": text,
                "translated_text": translated.text,
                "target_lang": target_lang
            }
            producer.send(TRANSLATED_TEXT_TOPIC, result)
            print(f"Translated: {result}")
        except Exception as e:
            print(f"Translation failed: {e}")
    producer.close()

# Kafka Consumer to Display Translated Text
def consume_translated_text():
    consumer = KafkaConsumer(
        TRANSLATED_TEXT_TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    print("Listening for translated text...")
    for message in consumer:
        data = message.value
        print(f"Translated Message: {data}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka Multilingual Translation Service")
    parser.add_argument("--mode", type=str, choices=["produce", "translate", "display"], required=True,
                        help="Mode of operation: produce, translate, display")
    args = parser.parse_args()

    if args.mode == "produce":
        produce_raw_text()
    elif args.mode == "translate":
        consume_and_translate()
    elif args.mode == "display":
        consume_translated_text()
