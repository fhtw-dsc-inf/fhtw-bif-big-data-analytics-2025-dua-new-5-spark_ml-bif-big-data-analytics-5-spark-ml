
import faust
from faust.types.auth import AuthProtocol
import ssl
from river import compose, metrics, reco
from utils import ccloud_lib
from faust_music_events import MusicEvent
import pickle

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
app = faust.App('music_stream_processor',
                topic_replication_factor=3,
                topic_partitions=1,
                broker=f"kafka://{kafka_app_config['bootstrap.servers']}",
                value_serializer='json',
                store='rocksdb://',
                broker_credentials=creds)

# Define a Kafka topic with MusicEvent as the value type
topic = app.topic('music_streams', value_type=MusicEvent)
song_plays = app.Table('song_plays', default=int)
model_table = app.Table('model_state', default=dict)

# Define a class to maintain state
class MusicRecommender:
    def __init__(self):
        self.model = reco.BiasedMF(n_factors=10)
        self.metric = metrics.MAE()

    def update_model(self, user_id, item_id, rating):
        # Make a prediction
        prediction = self.model.predict_one(user=user_id, item=item_id)

        # Update the metric
        self.metric.update(rating, prediction)

        # Train the model
        self.model.learn_one(user=user_id, item=item_id, y=rating)

        # Store the model state
        model_table['model'] = pickle.dumps(self.model)
        model_table['metric'] = pickle.dumps(self.metric)

recommender = MusicRecommender()

# Load model state from RocksDB if available
if 'model' in model_table and 'metric' in model_table:
    recommender.model = pickle.loads(model_table['model'])
    recommender.metric = pickle.loads(model_table['metric'])

# Define a stream processor
@app.agent(topic)
async def process(stream):
    async for event in stream:
        user_id = int(event.userId)
        item_id = int(event.track_id)
        rating = 1 if event.page == 'NextSong' else 0

        # Update model with new data
        recommender.update_model(user_id, item_id, rating)

        song_plays[event.userId] += 1
        print(f'User {event.userId} has listened to {song_plays[event.userId]} songs.')
