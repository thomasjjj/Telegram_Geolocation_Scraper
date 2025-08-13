import os
import configparser
import logging
from pathlib import Path
from dotenv import load_dotenv, find_dotenv, set_key


class Config:
    """Configuration manager for the Telegram Coordinates Scraper.

    This class handles loading configuration from:
    1. Default values
    2. .env file
    3. Config file
    4. Environment variables
    5. Command-line arguments (passed directly)

    Each level overrides the previous one.
    """

    def __init__(self, config_file=None):
        """Initialize configuration with optional config file path.

        Args:
            config_file (str, optional): Path to the config file. If None,
                                         looks for config.ini in standard locations.
        """
        self.config = configparser.ConfigParser()

        # Set default values
        self._set_defaults()

        # Try to load from .env file
        self._load_from_env_file()

        # Try to find and load the config file
        self.config_file = self._find_config_file(config_file)
        if self.config_file:
            self.config.read(self.config_file)

        # Override with environment variables
        self._load_from_env()

        # Ensure results folder exists
        self._ensure_results_folder_exists()

    def _set_defaults(self):
        """Set default configuration values."""
        self.config['telegram'] = {
            'api_id': '',
            'api_hash': '',
            'session_name': 'session_name'
        }

        self.config['search'] = {
            'search_terms': ('"E", "N", "S", "W", "Coordinates", "Geolocation", '
                             '"Geolocated", "located", "location", "gps", '
                             '"Геолокація", "Геолокований", "Розташований", "Місцезнаходження", '
                             '"Геолокация", "Геолокированный", "Расположенный", "Местоположение", "Координати"')
        }

        self.config['output'] = {
            'csv_file': 'coordinates_search_results.csv',
            'results_folder': 'results'
        }

        self.config['logging'] = {
            'log_file': 'telegram_search.log',
            'log_level': 'INFO'
        }

    def _find_env_file(self):
        """Find the .env file in standard locations.

        Returns:
            str: Path to the .env file, or None if not found.
        """
        # First check if .env already exists
        env_file = find_dotenv(usecwd=True)
        if env_file:
            return env_file

        # Define standard locations to check
        locations = [
            # Current directory
            os.path.join(os.getcwd(), '.env'),
            # User's home directory
            os.path.join(str(Path.home()), '.telegram_coordinates_scraper', '.env'),
            # Package directory
            os.path.join(os.path.dirname(__file__), '..', '.env')
        ]

        for location in locations:
            if os.path.isfile(location):
                return location

        return None

    def _load_from_env_file(self):
        """Load configuration from .env file."""
        # Try to find and load the .env file
        env_file = self._find_env_file()
        if env_file:
            load_dotenv(env_file)
            logging.info(f"Loaded configuration from .env file: {env_file}")
            return env_file
        return None

    def _find_config_file(self, config_file):
        """Find the configuration file in standard locations.

        Args:
            config_file (str, optional): User-provided config file path.

        Returns:
            str: Path to the config file, or None if not found.
        """
        # If a config file was explicitly specified, use it
        if config_file and os.path.isfile(config_file):
            return config_file

        # Places to look for config.ini
        locations = [
            # Current directory
            os.path.join(os.getcwd(), 'config.ini'),
            # User's home directory
            os.path.join(str(Path.home()), '.telegram_coordinates_scraper', 'config.ini'),
            # /etc for Linux/Mac
            '/etc/telegram_coordinates_scraper/config.ini',
            # Package directory
            os.path.join(os.path.dirname(__file__), '..', 'config.ini')
        ]

        for location in locations:
            if os.path.isfile(location):
                return location

        return None

    def _load_from_env(self):
        """Load configuration from environment variables."""
        # Telegram API credentials
        api_id = os.environ.get('TELEGRAM_API_ID')
        if api_id:
            self.config['telegram']['api_id'] = api_id

        api_hash = os.environ.get('TELEGRAM_API_HASH')
        if api_hash:
            self.config['telegram']['api_hash'] = api_hash

        session_name = os.environ.get('TELEGRAM_SESSION_NAME')
        if session_name:
            self.config['telegram']['session_name'] = session_name

        # Search terms
        search_terms = os.environ.get('TELEGRAM_SEARCH_TERMS')
        if search_terms:
            self.config['search']['search_terms'] = search_terms

        # Output file
        csv_file = os.environ.get('TELEGRAM_COORDINATES_CSV_FILE')
        if csv_file:
            self.config['output']['csv_file'] = csv_file

        # Results folder
        results_folder = os.environ.get('TELEGRAM_COORDINATES_RESULTS_FOLDER')
        if results_folder:
            self.config['output']['results_folder'] = results_folder

        # Logging
        log_file = os.environ.get('TELEGRAM_COORDINATES_LOG_FILE')
        if log_file:
            self.config['logging']['log_file'] = log_file

        log_level = os.environ.get('TELEGRAM_COORDINATES_LOG_LEVEL')
        if log_level:
            self.config['logging']['log_level'] = log_level

    def _ensure_results_folder_exists(self):
        """Ensure that the results folder exists, creating it if necessary."""
        results_folder = self.get_results_folder()
        if not os.path.exists(results_folder):
            try:
                os.makedirs(results_folder)
                logging.info(f"Created results folder: {results_folder}")
            except OSError as e:
                logging.error(f"Failed to create results folder: {e}")

    def create_env_file(self, api_id, api_hash, session_name=None):
        """Create a .env file with the provided API credentials.

        Args:
            api_id (str): Telegram API ID
            api_hash (str): Telegram API hash
            session_name (str, optional): Session name for Telegram client

        Returns:
            str: Path to the created .env file
        """
        # Default location for .env file
        env_file = os.path.join(os.getcwd(), '.env')

        # Ensure directory exists
        os.makedirs(os.path.dirname(env_file) if os.path.dirname(env_file) else '.', exist_ok=True)

        # Create the .env file if it doesn't exist yet
        if not os.path.exists(env_file):
            with open(env_file, 'w') as f:
                f.write("# Telegram API Credentials\n")
                f.write(f"TELEGRAM_API_ID={api_id}\n")
                f.write(f"TELEGRAM_API_HASH={api_hash}\n")

                if session_name:
                    f.write(f"\n# Session Configuration\n")
                    f.write(f"TELEGRAM_SESSION_NAME={session_name}\n")

                f.write("\n# Search Configuration\n")
                f.write(
                    'TELEGRAM_SEARCH_TERMS="E,N,S,W,Coordinates,Geolocation,Geolocated,located,location,gps,Геолокація,Геолокований,Розташований,Місцезнаходження,Геолокация,Геолокированный,Расположенный,Местоположение,Координати"\n')

                f.write("\n# Output Configuration\n")
                f.write("TELEGRAM_COORDINATES_CSV_FILE=coordinates_results.csv\n")
                f.write("TELEGRAM_COORDINATES_RESULTS_FOLDER=results\n")

                f.write("\n# Logging Configuration\n")
                f.write("TELEGRAM_COORDINATES_LOG_FILE=telegram_search.log\n")
                f.write("TELEGRAM_COORDINATES_LOG_LEVEL=INFO\n")

            logging.info(f"Created new .env file at {env_file}")
        else:
            # Update existing .env file
            set_key(env_file, "TELEGRAM_API_ID", str(api_id))
            set_key(env_file, "TELEGRAM_API_HASH", api_hash)
            if session_name:
                set_key(env_file, "TELEGRAM_SESSION_NAME", session_name)
            logging.info(f"Updated existing .env file at {env_file}")

        # Reload environment variables
        load_dotenv(env_file)
        return env_file

    def setup_logging(self):
        """Set up logging based on the configuration."""
        log_file = self.config['logging']['log_file']
        log_level_str = self.config['logging']['log_level']
        log_level = getattr(logging, log_level_str.upper(), logging.INFO)

        # Create handlers
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)

        # Configure root logger
        logging.basicConfig(
            level=log_level,
            handlers=[stream_handler, file_handler]
        )

        logging.info(f"Logging initialized at level {log_level_str}")

    def get_telegram_credentials(self):
        """Get Telegram API credentials, prompting user if not available.

        Returns:
            tuple: (api_id, api_hash)
        """
        api_id = self.config['telegram']['api_id']
        api_hash = self.config['telegram']['api_hash']

        if not api_id or not api_hash:
            logging.info("Telegram API credentials not found in config or environment")
            try:
                if not api_id:
                    api_id = input("Enter your Telegram API ID: ")
                if not api_hash:
                    api_hash = input("Enter your Telegram API Hash: ")

                # Ask if user wants to save credentials to .env file
                save_to_env = input("Would you like to save these credentials to a .env file? (y/n): ").lower()
                if save_to_env == 'y' or save_to_env == 'yes':
                    session_name = input(
                        "Enter a session name (press Enter for default 'coordinates_scraper_session'): ")
                    if not session_name:
                        session_name = "coordinates_scraper_session"
                    self.create_env_file(api_id, api_hash, session_name)
                    print(f"Credentials saved to .env file. You won't need to enter them again.")

                logging.info("Successfully obtained API credentials from user input")
            except ValueError as e:
                logging.error("Invalid input for API credentials. Ensure the API ID is a number.")
                raise e
        else:
            try:
                api_id = int(api_id)
                logging.info("Successfully loaded API credentials from configuration")
            except ValueError as e:
                logging.error("Invalid API ID in configuration. Ensure it's a number.")
                raise e

        return api_id, api_hash

    def get_session_name(self):
        """Get the Telegram session name.

        Returns:
            str: Session name
        """
        return self.config['telegram']['session_name']

    def get_search_terms(self):
        """Get the search terms as a list.

        Returns:
            list: Search terms
        """
        terms_str = self.config['search']['search_terms']
        # Parse the string into a list
        # Strip quotes and whitespace from each term
        return [term.strip(' "\'') for term in terms_str.split(',')]

    def get_results_folder(self):
        """Get the path to the results folder.

        Returns:
            str: Path to the results folder
        """
        return self.config['output']['results_folder']

    def get_output_file(self):
        """Get the path to the CSV output file.

        This combines the results folder with the CSV filename.

        Returns:
            str: Path to the CSV file
        """
        results_folder = self.get_results_folder()
        csv_filename = self.config['output']['csv_file']
        return os.path.join(results_folder, csv_filename)

    def update_config(self, section, key, value):
        """Update a configuration value.

        Args:
            section (str): Configuration section
            key (str): Configuration key
            value (str): New value
        """
        if section not in self.config:
            self.config[section] = {}

        self.config[section][key] = str(value)

        # If we have a config file, update it
        if self.config_file:
            with open(self.config_file, 'w') as f:
                self.config.write(f)
