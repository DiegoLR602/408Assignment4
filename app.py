from multiprocessing.reduction import DupFd
from db_operations import db_operations
from helper import helper

#use DB Operations class to create object to help us connect to database
db_ops = db_operations("./chinook.db")
data = helper.data_cleaner("songs.csv")

#Fills the songs table with all data from songs.csv if it is empty
def pre_process():
    if is_empty():  
        attribute_count = len(data[0])
        placeholders = ("?,"*attribute_count)[:-1]
        query = "INSERT INTO songs VALUES("+placeholders+")"
        db_ops.bulk_insert(query,data)

    print("Do you want to insert new songs? 1) Yes, 2) No: ")
    user_input = helper.get_choice([1,2])
    if(user_input == 1):
        print("Enter the path for the songs you want to insert")
        path = input("PATH: ")
        new_data = helper.data_cleaner(path)
        # This would be for bonus 1 (doesn't work):
        # for tup in new_data:
        #     if (songID(tup[1]) == tup[0]):
        #         new_data.remove(tup)
        attribute_count = len(data[0])
        placeholders = ("?,"*attribute_count)[:-1]
        query = "INSERT INTO songs VALUES("+placeholders+")"

        db_ops.bulk_insert(query,new_data)

#Return if songs table is empty
def is_empty():
    query = '''
    SELECT COUNT(*)
    FROM songs;
    '''

    result = db_ops.single_record(query)
    return result == 0

#print all options for user and return their choice
def options():
    print('''
    Select from the following menu options:\n
    1) Find Songs by Artist\n
    2) Find Songs by Genre\n
    3) Find Songs by Feature\n
    4) Update Song Information\n
    5) Delete Song by Name\n
    6) Remove All Null Values\n
    7) Exit
    ''')
    return helper.get_choice([1,2,3,4,5,6,7])

#print all artists, allow a user to select one, then return all songs from that artist
def search_by_artist():
    #find all of the artists on the playlist
    query = '''
    SELECT DISTINCT Artist
    FROM songs;
    '''

    print("Artists in playlist: ")
    artists = db_ops.single_attribute(query)

    #print artists to user and return their choice
    choices = {}
    for i in range(len(artists)):
        print(i, artists[i])
        choices[i] = artists[i]
    index = helper.get_choice(choices.keys())

    #user can ask to see 1, 5, or all songs
    print("How many songs do you want returned for"+choices[index]+"?")
    print("Enter 1, 5, or 0 for all songs")
    num = helper.get_choice([1,5,0])

    #prepare query and show results
    query = "SELECT DISTINCT name FROM songs WHERE Artist=:artist ORDER BY RANDOM()"
    dictionary = {"artist":choices[index]}
    if num != 0:
        query +=" LIMIT:lim"
        dictionary["lim"] = num
    helper.pretty_print(db_ops.name_placeholder_query(query, dictionary))

# option 2, search table for songs by genre
def search_by_genre():
    #get all genres and allow user to choose one
    query = '''
    SELECT DISTINCT Genre
    FROM songs;
    '''

    print("Genres in playlist:")
    genres = db_ops.single_attribute(query)

    choices = {}
    for i in range(len(genres)):
        print(i,genres[i])
        choices[i] = genres[i]
    index = helper.get_choice(choices.keys())

    #how many records
    print("How many songs do you want returned for "+choices[index]+"?")
    print("Enter 1, 5, or 0 for all songs")
    num = helper.get_choice([1,5,0])

    #run query and show results
    query = "SELECT DISTINCT name FROM songs WHERE Genre =:genre ORDER BY RANDOM()"
    dictionary = {"genre":choices[index]}
    if num != 0:
        query +=" LIMIT:lim"
        dictionary["lim"] = num
    helper.pretty_print(db_ops.name_placeholder_query(query, dictionary))

#Option 3, search by specific feature
def search_by_feature():
    #give users features and return choice
    features = ['Danceability', 'Liveness', 'Loudness']
    choices = {}
    for i in range(len(features)):
        print(i, features[i])
        choices[i] = features[i]
    index = helper.get_choice(choices.keys())

    #how many records
    print("How many songs do you want returned for "+choices[index]+"?")
    print("Enter 1, 5, or 0 for all songs")
    num = helper.get_choice([1,5,0])

    print("Do you want results sorted in ASC or DESC?")
    order = input("ASC or DESC: ")

    #prepare query and show results
    query = "SELECT DISTINCT Name FROM songs ORDER BY "+choices[index]+" "+order
    dictionary = {}
    if num!=0:
        query += " LIMIT :lim"
        dictionary["lim"] = num
    helper.pretty_print(db_ops.name_placeholder_query(query,dictionary))

# Option 5, delete specified song
def delete_song():
    print("Enter the song you want to delete")
    deleted_song_name = input("SONG: ")
    song_ID = songID(deleted_song_name)
    query = '''
    DELETE
    FROM songs
    WHERE songID = '%s'
    ''' % (song_ID)

    db_ops.execute(query)

#Helper function to get songID
def songID(Name):
    try:
        query = '''
        SELECT songID
        FROM songs
        WHERE Name = '%s'
        ''' % Name
        return db_ops.single_record(query)
    except: 
        print("The song", Name, "does not exist")
        return "-1"

#Option 4, update song information
def update_song_information():
    song_name = input("What song do you want to modify?: ")
    update_list = '''
    1) Song Name 
    2) Album Name 
    3) Artist Name 
    4) Release Date 
    5) Explicit
    '''

    song_ID = songID(song_name)
    if (song_ID != "-1"):
        print(update_list)
        update_choice = helper.get_choice([1,2,3,4,5])

        match update_choice:
            case 1: 
                attribute = 'Name'
                user_attribute = input('New Song Name: ')
            case 2: 
                attribute = 'Album'
                user_attribute = input('New Album Name: ')
            case 3: 
                attribute = 'Artist'
                user_attribute = input('New Artist Name: ')
            case 4: 
                attribute = 'releaseDate'
                user_attribute = input('New Release Date: ')
            case 5:
                attribute = 'Explicit'
                print('New Explicit rating (0 = not-explicit, 1 = explicit): ')
                user_attribute = str(helper.get_choice([0,1]))

        query = '''
        UPDATE songs 
        SET %s = '%s'
        WHERE songID = '%s'
        ''' % (attribute, user_attribute, song_ID)

        db_ops.execute(query)

#Bonus 3 - remove all null values from table
def remove_null():
    query = '''
    DELETE FROM songs
    WHERE Name IS NULL
            OR Artist IS NULL
            OR Album IS NULL
            OR releaseDate IS NULL
            OR Genre IS NULL
            OR Explicit IS NULL
            OR Duration IS NULL
            OR Energy IS NULL
            OR Danceability IS NULL
            OR Acousticness IS NULL
            OR Liveness IS NULL
            OR Loudness IS NULL;
    '''
    db_ops.execute(query)

#Main
#Introduce user to their playlist
print("Welcome to your playlist!")
#make sure the songs table is populated
pre_process()
#allow user to repeatedly interface with the application until they quit
while True:
    match options():
        case 1:
            search_by_artist()
        case 2:
            search_by_genre()
        case 3:
            search_by_feature()
        case 4:
            update_song_information()
        case 5:
            delete_song()
        case 6:
            remove_null()
        case 7:
            print("Goodbye!")
            break

#close connection
db_ops.destructor()
