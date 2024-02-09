from flask import Flask, request, jsonify, abort
from flask_sqlalchemy import SQLAlchemy
from flask_caching import Cache
from sqlalchemy import text
import os
from datetime import datetime

basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)

# Configuration for Flask-SQLAlchemy
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'tournament.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Configuration for Flask-Caching
app.config['CACHE_TYPE'] = 'simple'
cache = Cache(app)

# Define the Score model
class Score(db.Model):
    __tablename__ = 'score'
    version = db.Column(db.Integer, primary_key=True)
    tournament_type = db.Column(db.String(20), primary_key=True)
    age_group = db.Column(db.String(10), primary_key=True)
    standing_level = db.Column(db.String(10), primary_key=True)
    ranking_points = db.Column(db.Integer)

# Define the USABPlayer model
class USABPlayer(db.Model):
    __tablename__ = 'usab_player'
    usab_id = db.Column(db.Integer, primary_key=True)
    player_name = db.Column(db.String(50))
    birth_year = db.Column(db.Integer)
    gender = db.Column(db.String(1))

# Define the Tournament model
class Tournament(db.Model):
    __tablename__ = 'tournament'
    tournament_id = db.Column(db.Integer, primary_key=True)
    tournament_name = db.Column(db.String(50))
    tournament_type = db.Column(db.String(20), db.ForeignKey('score.tournament_type'))
    description = db.Column(db.String(255))
    location = db.Column(db.String(20))
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)

# Define the TournamentPlayer model with the combination of tournament_id and tournament_player_id as the primary key
class TournamentPlayer(db.Model):
    __tablename__ = 'tournament_player'
    tournament_id = db.Column(db.Integer, db.ForeignKey('tournament.tournament_id'), primary_key=True)
    tournament_player_id = db.Column(db.Integer, primary_key=True)
    usab_id = db.Column(db.Integer, db.ForeignKey('usab_player.usab_id'))
    player_name = db.Column(db.String(50))
    tournament = db.relationship('Tournament', backref=db.backref('tournament_players', lazy=True))
    usab_player = db.relationship('USABPlayer', backref=db.backref('tournament_participations', lazy=True))

# Define the TournamentPlayerPerformance model
class TournamentPlayerPerformance(db.Model):
    __tablename__ = 'tournament_player_performance'
    tournament_id = db.Column(db.Integer, primary_key=True)
    tournament_player_id = db.Column(db.Integer, primary_key=True)
    age_group = db.Column(db.String(10), primary_key=True)
    event_type = db.Column(db.String(20), primary_key=True)
    standing_level = db.Column(db.String(10), primary_key=True)
    tournament_player = db.relationship('TournamentPlayer', backref=db.backref('tournament_performances', lazy=True))
    __table_args__ = (
        db.ForeignKeyConstraint(['tournament_id', 'tournament_player_id'], ['tournament_player.tournament_id', 'tournament_player.tournament_player_id']),
    )

# Cache keys for player and tournament lists
PLAYER_DICT_CACHE_KEY = 'usab_player_dict'
PLAYER_LIST_CACHE_KEY = 'usab_player_list'
TOURNAMENT_DICT_CACHE_KEY = 'tournament_dict'
TOURNAMENT_LIST_CACHE_KEY = 'tournament_list'
SCORES_DICT_CACHE_KEY = 'score_dict'
CURRENT_SCORES_DICT_CACHE_KEY = "score_dict_current"

RANKING_SQL = """
    WITH player_ranked_score AS (
        SELECT
        usab_id, player_name, tournament_name, end_date, event_u_age, event_type, standing_level, ranking_points, player_age,
        ROW_NUMBER() OVER (PARTITION BY usab_id, event_type ORDER BY ranking_points DESC) AS rank
        FROM
        usab_player_tournament_score
        WHERE event_u_age <= :event_u_age
    )
    SELECT usab_id, player_name, SUM(ranking_points) AS total_score
    FROM player_ranked_score
    WHERE player_age < :event_u_age AND event_type=:event_type AND rank <=4
    GROUP BY usab_id
    ORDER BY total_score DESC
"""

ALL_EVENT_TYPES = {'BS', 'GS', 'BD', 'GD', 'XD'}
ALL_AGE_GROUPS = {'U11', 'U13', 'U15', 'U17', 'U19'}

def get_all_version_score_dict():
    score_dict = cache.get(SCORES_DICT_CACHE_KEY)

    if score_dict is None:
        # If not in cache, fetch from database
        scores = Score.query.all()

        # Format the results as a list of dictionaries
        score_dict = {}
        for score in scores:
            if score.version not in score_dict:
                score_dict[score.version] = {}
            score_dict[score.version][(score.tournament_type, score.age_group, score.standing_level)] = score.ranking_points

        # Cache the result for subsequent requests
        cache.set(SCORES_DICT_CACHE_KEY, score_dict)
    return score_dict

def get_score_dict(version):
    cache_key = f'score_dict_v{version}'
    versioned_score_dict = cache.get(cache_key)

    if versioned_score_dict is None:
        all_version_score_dict = get_all_version_score_dict()
        if version in all_version_score_dict:
            versioned_score_dict = all_version_score_dict[version]
            # Cache the result for subsequent requests
            cache.set(cache_key, versioned_score_dict)
        else:
            abort(400, 'Bad Request: invalid version parameter')
    return versioned_score_dict

def get_current_score_dict():
    current_score_dict = cache.get(CURRENT_SCORES_DICT_CACHE_KEY)

    if current_score_dict is None:
        all_version_score_dict = get_all_version_score_dict()
        current_version = max(all_version_score_dict.keys())
        current_score_dict = all_version_score_dict[current_version]
        # Cache the result for subsequent requests
        cache.set(CURRENT_SCORES_DICT_CACHE_KEY, current_score_dict)
    return current_score_dict

def get_usab_players_dict():
    # Try to get player list from cache
    usab_player_dict = cache.get(PLAYER_DICT_CACHE_KEY)

    if usab_player_dict is None:
        # If not in cache, fetch from database
        usab_players = USABPlayer.query.filter(USABPlayer.usab_id != 0).all()

        usab_player_dict = {}
        # Format the results as a list of dictionaries
        for player in usab_players:
            usab_player_dict[player.usab_id] = {'player_name': player.player_name,
                                           'birth_year': player.birth_year,
                                           'gender': player.gender}

        # Cache the result for subsequent requests
        cache.set(PLAYER_DICT_CACHE_KEY, usab_player_dict)
    return usab_player_dict

def get_tournaments_dict():
    # Try to get tournament list from cache
    tournament_dict = cache.get(TOURNAMENT_DICT_CACHE_KEY)

    if tournament_dict is None:
        # If not in cache, fetch from database
        tournaments = Tournament.query.all()

        tournament_dict = {}
        # Format the results as a list of dictionaries
        for tournament in tournaments:
            tournament_dict[tournament.tournament_id] = {
                            'tournament_name': tournament.tournament_name,
                            'tournament_type': tournament.tournament_type,
                            'description': tournament.description,
                            'location': tournament.location,
                            'start_date': str(tournament.start_date),
                            'end_date': str(tournament.end_date)}

        # Cache the result for subsequent requests
        cache.set(TOURNAMENT_DICT_CACHE_KEY, tournament_dict)

    return tournament_dict

# Custom key function to extract the first number before '-'
def sort_by_standing_level(item):
    return int(item['standing_level'].split('-')[0])

def sort_by_end_date(item):
    return item['end_date']

def sort_by_usab_id(item):
    return item['usab_id']

def sort_by_tournament_player_id(item):
    return item['tournament_player_id']

def sort_by_score(item):
    return item['score']

def is_valid_date(date_string):
    try:
        # Attempt to parse the date string
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        # Raised if the format is incorrect
        return False

def filter_performance(performance, request_args):
    min_date_filter = request_args.get('min_date')
    max_date_filter = request_args.get('max_date')
    event_type_filter = request_args.get('event_type')
    age_group_filter = request_args.get('age_group')
    if event_type_filter != None and event_type_filter.upper() != performance['event_type']:
        return False
    if age_group_filter != None and age_group_filter.upper() != performance['age_group']:
        return False
    if min_date_filter != None and min_date_filter > performance['end_date']:
        return False
    if max_date_filter != None and max_date_filter < performance['end_date']:
        return False
    return True

def filter_tournament_event_performance(performance, request_args):
    event_type_filter = request_args.get('event_type')
    age_group_filter = request_args.get('age_group')
    if event_type_filter != None and event_type_filter.upper() != performance['event_type']:
        return False
    if age_group_filter != None and age_group_filter.upper() != performance['age_group']:
        return False
    return True

# Endpoint to fetch all USAB players
@app.route('/api/v1/scores', methods=['GET'])
def get_scores():
    version = request.args.get('version')
    if version == None:
        # Try to get player list from cache
        score_dict = get_current_score_dict()
    else:
        if not version.isdigit():
            abort(400, 'Bad Request: invalid version query parameter')
        else:
            score_dict = get_score_dict(int(version))

    score_list = [{'tournament_type': tournament_type, 'age_group': age_group,
                    'standing_level': standing_level, 'ranking_points': ranking_points} for (tournament_type, age_group, standing_level), ranking_points in score_dict.items()]

    return jsonify(score_list)

# Endpoint to fetch all USAB players
@app.route('/api/v1/players', methods=['GET'])
def get_usab_players():
    # Try to get player list from cache
    usab_player_list = cache.get(PLAYER_LIST_CACHE_KEY)

    if usab_player_list is None:
        usab_player_dict = get_usab_players_dict()

        # Format the results as a list of dictionaries
        usab_player_list = [{'usab_id': usab_id,
                             'player_name': player['player_name'],
                             'birth_year': player['birth_year'],
                             'gender': player['gender']} for usab_id, player in usab_player_dict.items()]

        usab_player_list = sorted(usab_player_list, key=sort_by_usab_id)

        # Cache the result for subsequent requests
        cache.set(PLAYER_LIST_CACHE_KEY, usab_player_list)

    return jsonify(usab_player_list)

# Endpoint to fetch all USAB players
@app.route('/api/v1/player/<usab_id>', methods=['GET'])
def get_usab_player(usab_id):
    if not usab_id.isdigit():
        abort(400, "Bad request: invalid player id")
    usab_player_dict = get_usab_players_dict()
    usab_id_int = int(usab_id)
    if usab_id_int in usab_player_dict:
        return jsonify(usab_player_dict[usab_id_int])
    else:
        abort(404, 'Not found')

# Endpoint to fetch all tournaments
@app.route('/api/v1/tournaments', methods=['GET'])
def get_all_tournaments():
    # Try to get tournament list from cache
    # Try to get tournament list from cache
    tournament_list = cache.get(TOURNAMENT_LIST_CACHE_KEY)

    if tournament_list is None:
        # If not in cache, fetch from database
        tournament_dict = get_tournaments_dict()

        # Format the results as a list of dictionaries
        tournament_list = [{'tournament_id': tournament_id,
                            'tournament_name': tournament['tournament_name'],
                            'tournament_type': tournament['tournament_type'],
                            'description': tournament['description'],
                            'location': tournament['location'],
                            'start_date': tournament['start_date'],
                            'end_date': tournament['end_date']} for tournament_id, tournament in tournament_dict.items()]

        tournament_list = sorted(tournament_list, key=sort_by_end_date, reverse=True)

        # Cache the result for subsequent requests
        cache.set(TOURNAMENT_LIST_CACHE_KEY, tournament_list)

    return jsonify(tournament_list)

# Endpoint to fetch all tournaments
@app.route('/api/v1/tournament/<tournament_id>', methods=['GET'])
def get_tournament(tournament_id):
    # Try to get tournament list from cache
    tournament_dict = get_tournaments_dict()
    if tournament_id in tournament_dict:
        return jsonify(tournament_dict[tournament_id])
    else:
        abort(404, 'Not found')

# Endpoint to fetch all players in a specific tournament
@app.route('/api/v1/tournament/<tournament_id>/players', methods=['GET'])
def get_tournament_players(tournament_id):
    # Try to get player list from cache
    cache_key = f'tournament_{tournament_id}_players_list'
    tournament_players_list = cache.get(cache_key)

    if tournament_players_list is None:
        # If not in cache, fetch from database
        tournament_players = TournamentPlayer.query.filter_by(tournament_id=tournament_id).all()

        # Format the results as a list of dictionaries
        tournament_players_list = [{'tournament_player_id': player.tournament_player_id,
                                    'usab_id': player.usab_id, 'player_name': player.player_name} for player in tournament_players]

        tournament_players_list = sorted(tournament_players_list, key=sort_by_tournament_player_id)

        # Cache the result for subsequent requests
        cache.set(cache_key, tournament_players_list)

    return jsonify(tournament_players_list)

# Endpoint to fetch all players in a specific tournament
@app.route('/api/v1/tournament/<tournament_id>/performance', methods=['GET'])
def get_tournament_event_performance(tournament_id):
    event_type = request.args.get('event_type')
    if event_type != None:
        event_type = event_type.upper()
        if not event_type in ALL_EVENT_TYPES:
            abort(400, 'Bad Request: invalid event_type query parameter')

    age_group = request.args.get('age_group')
    if age_group != None:
        age_group = age_group.upper()
        if not age_group in ALL_AGE_GROUPS:
            abort(400, 'Bad Request: invalid age_group query parameter')

    # Try to get player list from cache
    cache_key = f'tournament_{tournament_id}_performance_list'
    tournament_event_performance_list = cache.get(cache_key)

    if tournament_event_performance_list is None:
        # If not in cache, fetch from database
        tournament_event_performance = (TournamentPlayerPerformance.query
                                             .join(TournamentPlayer, (
                                                 (TournamentPlayerPerformance.tournament_id == TournamentPlayer.tournament_id) &
                                                 (TournamentPlayerPerformance.tournament_player_id == TournamentPlayer.tournament_player_id)
                                             ))
                                             .join(USABPlayer, (
                                                 (TournamentPlayer.usab_id == USABPlayer.usab_id)
                                             ))
                                             .filter(TournamentPlayerPerformance.tournament_id == tournament_id)
                                             .all())

        # Format the results as a list of dictionaries
        tournament_event_performance_list = [{'age_group': performance.age_group,
                                              'event_type': performance.event_type,
                                              'usab_id': performance.tournament_player.usab_id,
                                              'player_name': performance.tournament_player.player_name,
                                              'standing_level': performance.standing_level} for performance in tournament_event_performance]
        # Sort the list using the custom key function
        tournament_event_performance_list = sorted(tournament_event_performance_list, key=sort_by_standing_level)

        # Cache the result for subsequent requests
        cache.set(cache_key, tournament_event_performance_list)

    filtered_performance_list = []
    for performance in tournament_event_performance_list:
        if filter_tournament_event_performance(performance, request.args):
            new_perf = performance.copy()
            filtered_performance_list.append(new_perf)
    filtered_performance_list = sorted(filtered_performance_list, key=lambda x: (x['age_group'], x['event_type'], sort_by_standing_level(x)))

    return jsonify(filtered_performance_list)

# Endpoint to fetch all players in a specific tournament
@app.route('/api/v1/player/<usab_id>/performance', methods=['GET'])
def get_usab_player_performance(usab_id):
    min_date_filter = request.args.get('min_date')
    max_date_filter = request.args.get('max_date')
    event_type_filter = request.args.get('event_type')
    age_group_filter = request.args.get('age_group')

    if min_date_filter != None:
        if not is_valid_date(min_date_filter):
            abort(400, 'Bad Request: invalid min_date query parameter')

    if max_date_filter != None:
        if not is_valid_date(max_date_filter):
            abort(400, 'Bad Request: invalid max_date query parameter')

    if event_type_filter != None:
        event_type_filter = event_type_filter.upper()
        if not event_type_filter in ALL_EVENT_TYPES:
            abort(400, 'Bad Request: invalid event_type query parameter')

    if age_group_filter != None:
        age_group_filter = age_group_filter.upper()
        if not age_group_filter in ALL_AGE_GROUPS:
            abort(400, 'Bad Request: invalid age_group query parameter')

    # Try to get player list from cache
    cache_key = f'usab_player_{usab_id}_performance_list'
    performance_list = cache.get(cache_key)

    if performance_list is None:
        # If not in cache, fetch from database
        tournament_players_performances = (TournamentPlayerPerformance.query
                                           .join(TournamentPlayer, (
                                                (TournamentPlayerPerformance.tournament_id == TournamentPlayer.tournament_id) &
                                                (TournamentPlayerPerformance.tournament_player_id == TournamentPlayer.tournament_player_id)
                                            ))
                                           .filter(TournamentPlayer.usab_id == usab_id)
                                           .all())

        # Format the results as a list of dictionaries
        performance_list = [{'tournament_id': performance.tournament_player.tournament.tournament_id,
                             'tournament_name': performance.tournament_player.tournament.tournament_name,
                             'tournament_description': performance.tournament_player.tournament.description,
                             'tournament_type': performance.tournament_player.tournament.tournament_type,
                             'end_date': str(performance.tournament_player.tournament.end_date),
                             'event_type': performance.event_type,
                             'age_group': performance.age_group,
                             'player_name': performance.tournament_player.player_name,
                             'usab_id': performance.tournament_player.usab_id,
                             'standing_level': performance.standing_level} for performance in tournament_players_performances]

        # Cache the result for subsequent requests
        cache.set(cache_key, performance_list)

    score_version = request.args.get('score_version')
    if score_version == None:
        # Try to get player list from cache
        score_dict = get_current_score_dict()
    else:
        score_dict = get_score_dict(score_version)

    filtered_performance_list = []
    for performance in performance_list:
        if filter_performance(performance, request.args):
            new_perf = performance.copy()
            new_perf['score'] = score_dict[(new_perf['tournament_type'], new_perf['age_group'], new_perf['standing_level'])]
            filtered_performance_list.append(new_perf)
    filtered_performance_list = sorted(filtered_performance_list, key=sort_by_score, reverse=True)

    return jsonify(filtered_performance_list)

# Function to execute a SQL query and return the result
def execute_sql_query(query, params=None):
    result = db.session.execute(text(query), params)
    rows = result.fetchall()
    return rows

# Endpoint to fetch data from a complicated SQL query
@app.route('/api/v1/ranks', methods=['GET'])
def get_current_ranks():
    # Try to get player list from cache
    # Execute the SQL query using the function
    event_type = request.args.get('event_type')
    if event_type == None:
        abort(400, 'Bad Request: missing event_type query parameter')
    elif not event_type.upper() in ALL_EVENT_TYPES:
        abort(400, 'Bad Request: invalid event_type query parameter')

    age_group = request.args.get('age_group')
    if event_type == None:
        abort(400, 'Bad Request: missing age_group query parameter')
    elif not age_group.upper() in ALL_AGE_GROUPS:
        abort(400, 'Bad Request: invalid age_group query parameter')

    event_type = event_type.upper()
    age_group = age_group.upper()

    cache_key = f'tournament_{event_type}_{age_group}_ranks_list'
    ranks_list = cache.get(cache_key)
    if ranks_list is None:
        event_u_age = int(age_group[1:])
        parameter_values = {'event_u_age': event_u_age, 'event_type': event_type}  # Replace with actual parameter values
        result_rows = execute_sql_query(RANKING_SQL, parameter_values)

        # Format the results as a list of dictionaries
        results_list = [{'usab_id': row[0], 'player_name': row[1], 'scores': row[2], 'rank': index+1} for index, row in enumerate(result_rows)]
        cache.set(cache_key, ranks_list)

    return jsonify(results_list)

if __name__ == '__main__':
    app.run(debug=True)
