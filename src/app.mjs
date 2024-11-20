import MongoConnection from "./mongo/MongoConnection.mjs";
const DB_NAME = 'sample_mflix';
const COLLECTION_MOVIES_NAME = "movies";
const COLLECTION_COMMENTS_NAME = "comments";
const mongoConnection = new MongoConnection(process.env.MONGO_URI, DB_NAME);
const collectionMovies = mongoConnection.getCollection(COLLECTION_MOVIES_NAME);
const collectionComments = mongoConnection.getCollection(COLLECTION_COMMENTS_NAME);
collectionComments.aggregate([
  {
    '$lookup': {
      'from': 'movies', 
      'localField': 'movie_id', 
      'foreignField': '_id', 
      'as': 'moviesInfo'
    }
  }, {
    '$unwind': '$moviesInfo'
  }, {
    '$replaceRoot': {
      'newRoot': {
        '$mergeObjects': [
          '$$ROOT', {
            'title': '$moviesInfo.title'
          }
        ]
      }
    }
  }, {
    '$project': {
      'moviesInfo': 0
    }
  }, {
    '$limit': 5
  }
] 
).toArray().then(data=>console.log("========== first ==========",data));
collectionMovies.aggregate(
  [
    {
      '$facet': {
        'avgRating': [
          {
            '$group': {
              '_id': null, 
              'average_imdb_rating': {
                '$avg': '$imdb.rating'
              }
            }
          }
        ], 
        'comedyMovies': [
          {
            '$match': {
              'year': 2010, 
              'genres': 'Comedy'
            }
          }, {
            '$project': {
              'title': 1, 
              'imdb_rating': '$imdb.rating'
            }
          }
        ]
      }
    }, {
      '$unwind': '$comedyMovies'
    }, {
      '$project': {
        'title': '$comedyMovies.title', 
        'imdb_rating': '$comedyMovies.imdb_rating', 
        'avgRating': {
          '$arrayElemAt': [
            '$avgRating.average_imdb_rating', 0
          ]
        }
      }
    }, {
      '$match': {
        '$expr': {
          '$gt': [
            '$imdb_rating', '$avgRating'
          ]
        }
      }
    }, {
      '$project': {
        'title': 1
      }
    }
  ]
).toArray().then(data=>console.log("========== second ==========",data));;
