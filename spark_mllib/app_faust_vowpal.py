
import faust
import ssl
import vowpalwabbit
from utils import ccloud_lib
from faust_music_events import MusicEvent

# Read the Kafka configuration
kafka_app_config = ccloud_lib.read_ccloud_config("kafka.config")

# Set up SASL credentials
creds = faust.SASLCredentials(
    username=kafka_app_config['sasl.username'],
    password=kafka_app_config['sasl.password'],
    mechanism='PLAIN',
    ssl_context=ssl.create_default_context()
)

# Initialize the Faust app
app = faust.App(
    'music_stream_processor',
    broker=f"kafka://{kafka_app_config['bootstrap.servers']}",
    value_serializer='json',
    store='memory://',
    broker_credentials=creds
)

# Define a Kafka topic with MusicEvent as the value type
topic = app.topic('music_streams', value_type=MusicEvent)
song_plays = app.Table('song_plays', default=int)

# Define a class to maintain state and use Vowpal Wabbit
class MusicRecommender:
    def __init__(self):
        self.model = vowpalwabbit.Workspace("--cb 4", quiet=True)
        self.update_count = 0

    def update_model(self, user_id, item_id, rating):
        example = f"{rating} |u user_{user_id} |i item_{item_id}"
        self.model.learn(example)
        self.update_count += 1

        if self.update_count % 100 == 0:
            self.save_model()

    def predict(self, user_id, item_id):
        example = f"|u user_{user_id} |i item_{item_id}"
        prediction = self.model.predict(example)
        return prediction

    def save_model(self):
        self.model.save("cb.model")

    def load_model(self):
        try:
            self.model = vowpalwabbit.Workspace("--cb 4", quiet=True)
        except FileNotFoundError:
            pass

# Instantiate the recommender
recommender = MusicRecommender()
recommender.load_model()

# Define a stream processor
@app.agent(topic)
async def process(stream):
    async for event in stream:
        if event.page == "NextSong":
            rating = 1
        elif event.page == "Skip":
            rating = 0
        else:
            continue

        user_id = event.userId
        item_id = event.track_id

        recommender.update_model(user_id, item_id, rating)
        song_plays[event.userId] += 1
        print(f'User {event.userId} has listened to {song_plays[event.userId]} songs.')
