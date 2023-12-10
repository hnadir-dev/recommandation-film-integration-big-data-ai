import requests


class MovieInfo:
    
    def get_movie_poster_url(movie_name):
        # Replace 'YOUR_API_KEY' with your actual TMDb API key
        api_key = 'bba5904c90ac4619fc37e24c7067897a'
        
        # TMDb API endpoint for searching movies
        search_url = f'https://api.themoviedb.org/3/search/movie'
        
        # Parameters for the API request
        params = {
            'api_key': api_key,
            'query': movie_name
        }

        try:
            # Make the API request
            response = requests.get(search_url, params=params)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the JSON response
            data = response.json()

            # Extract the poster path for the first result
            if data['results']:
                poster_path = data['results'][0]['poster_path']
                
                # TMDb base URL for images
                base_url = 'https://image.tmdb.org/t/p/original'
                
                # Construct the full URL for the poster image
                poster_url = f'{base_url}{poster_path}'
                
                return poster_url
            else:
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error making TMDb API request: {e}")
            return None