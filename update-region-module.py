#!/usr/bin/env python

import sys
import math
import csv
import logging
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict
from enum import Enum
import seiscomp.core
import seiscomp.client
import seiscomp.datamodel as DM
import seiscomp.logging
from seiscomp.seismology import Regions
from logging.handlers import RotatingFileHandler

class DirectionType(Enum):
    """Direction granularity options"""
    CARDINAL = "cardinal"
    INTERCARDINAL = "intercardinal"
    DETAILED = "detailed"

    @classmethod
    def from_string(cls, s: str) -> 'DirectionType':
        """Convert string to DirectionType, case insensitive"""
        try:
            return cls[s.upper()]
        except KeyError:
            raise ValueError(f"Invalid direction type: {s}")

@dataclass
class LocationReference:
    """Enhanced location reference with validation"""
    name: str
    state: str
    country: str
    lat: float
    lon: float
    population: Optional[int] = None

    def __post_init__(self):
        if not (-90 <= self.lat <= 90):
            raise ValueError(f"Invalid latitude: {self.lat}")
        if not (-180 <= self.lon <= 180):
            raise ValueError(f"Invalid longitude: {self.lon}")
        if not self.name:
            raise ValueError("Location name cannot be empty")

    def __str__(self):
        return f"{self.name}, {self.state}, {self.country}"

class EventLocationConfig:
    """Configuration container for event location module"""
    def __init__(self):
        self.max_distance = 1000.0
        self.min_population = 50000
        self.direction_type = DirectionType.DETAILED
        self.debug_mode = False
        self.show_state = True
        self.show_country = True
        self.test_mode = False
        self.description_pattern = "{distance} km {direction} of {location}"

class EventLocationModule(seiscomp.client.Application):
    """SeisComP module for automated event location naming"""
    
    def __init__(self, argc: int, argv: List[str]):
        """Initialize the EventLocationModule with improved messaging configuration"""
        super().__init__(argc, argv)
        
        # Enhanced messaging configuration
        self.setMessagingEnabled(True)
#        self.setPrimaryMessagingGroup("LISTENER_GROUP")
        self.addMessagingSubscription("LOCATION")
        self.addMessagingSubscription("EVENT")
        self.setLoggingToStdErr(True)
        
        # Database configuration
        self.setDatabaseEnabled(True, True)
        self.setLoadStationsEnabled(True)
        self.setAutoApplyNotifierEnabled(True)
        
        # Initialize configuration
        self.config = EventLocationConfig()
        self._locations: Dict[str, LocationReference] = {}
        self.locations_file = None
        
        # Setup logging
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Initialize logging configuration"""
        logger = logging.getLogger("EventLocationModule")
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        
        # File handler
        file_handler = RotatingFileHandler(
            'event_location.log', maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger

    def createCommandLineDescription(self):
        """Define command line arguments"""
        self.commandline().addGroup("Module")
        self.commandline().addStringOption(
            "Module", "locations-file,L", "CSV file with reference locations")
        self.commandline().addDoubleOption(
            "Module", "max-distance,M", "Maximum distance to consider (km)")
        self.commandline().addIntOption(
            "Module", "min-population,P", "Minimum population for reference locations")
        self.commandline().addStringOption(
            "Module", "direction-type,D", 
            "Direction type (cardinal, intercardinal, detailed)")
        self.commandline().addOption(
            "Module", "test-mode,T", "Test mode - no database updates")
        self.commandline().addOption(
            "Module", "debug", "Enable debug logging")
        self.commandline().addOption(
            "Module", "no-state", "Don't include state in location description")
        self.commandline().addOption(
            "Module", "no-country", "Don't include country in location description")
        return True

    def init(self):
        """Initialize the module"""
        if not super().init():
            return False

        try:
            # Get configuration
            try:
                self.locations_file = self.commandline().optionString("locations-file")
            except:
                self.logger.error("No locations file specified, use --locations-file")
                return False

            # Optional configurations with defaults
            try:
                self.config.max_distance = self.commandline().optionDouble("max-distance")
            except:
                self.logger.info(f"Using default max distance of {self.config.max_distance} km")

            try:
                self.config.min_population = self.commandline().optionInt("min-population")
            except:
                self.logger.info(f"Using default minimum population of {self.config.min_population}")

            try:
                direction_str = self.commandline().optionString("direction-type")
                self.config.direction_type = DirectionType.from_string(direction_str)
            except:
                self.logger.info(f"Using default direction type: {self.config.direction_type.name}")

            # Boolean flags
            self.config.debug_mode = self.commandline().hasOption("debug")
            if self.config.debug_mode:
                self.logger.setLevel(logging.DEBUG)
                self.logger.debug("Debug logging enabled")

            self.config.test_mode = self.commandline().hasOption("test-mode")
            if self.config.test_mode:
                self.logger.info("Running in test mode - no database updates will be made")

            self.config.show_state = not self.commandline().hasOption("no-state")
            self.config.show_country = not self.commandline().hasOption("no-country")

            # Load locations database
            if not self._load_locations():
                return False

            # Subscribe to messaging groups
            if not self.connection().subscribe("LOCATION"):
                self.logger.error("Failed to subscribe to LOCATION group")
                return False

            if not self.connection().subscribe("EVENT"):
                self.logger.error("Failed to subscribe to EVENT group")
                return False

            self.logger.info("Module initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            return False

    def _load_locations(self) -> bool:
        """Load reference locations from CSV file"""
        try:
            with open(self.locations_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                required_fields = {'name', 'state', 'country', 'latitude', 'longitude', 'population'}
                
                if not required_fields.issubset(reader.fieldnames):
                    missing = required_fields - set(reader.fieldnames)
                    raise ValueError(f"Missing required fields: {missing}")
                
                for idx, row in enumerate(reader, start=2):
                    try:
                        loc = LocationReference(
                            name=row['name'].strip(),
                            state=row['state'].strip(),
                            country=row['country'].strip(),
                            lat=float(row['latitude']),
                            lon=float(row['longitude']),
                            population=int(row.get('population', 0))
                        )
                        if loc.population >= self.config.min_population:
                            self._locations[f"{loc.name}_{loc.state}_{loc.country}"] = loc
                    except (ValueError, KeyError) as e:
                        self.logger.warning(f"Invalid row {idx}: {e}")
                
                if not self._locations:
                    raise ValueError("No valid locations loaded")
                
                self.logger.info(f"Loaded {len(self._locations)} locations")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to load locations: {e}")
            return False

    def calculateDistance(self, ref_lat: float, ref_lon: float,
                         event_lat: float, event_lon: float) -> Tuple[float, float]:
        """Calculate distance and bearing using Haversine formula"""
        try:
            R = 6371  # Earth radius in kilometers
            lat1, lon1 = map(math.radians, [ref_lat, ref_lon])
            lat2, lon2 = map(math.radians, [event_lat, event_lon])

            dlat = lat2 - lat1
            dlon = lon2 - lon1

            a = math.sin(dlat/2)**2 + math.cos(lat1) * \
                math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            distance = R * c

            # Calculate bearing
            y = math.sin(lon2 - lon1) * math.cos(lat2)
            x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * \
                math.cos(lat2) * math.cos(lon2 - lon1)
            bearing = math.degrees(math.atan2(y, x))
            bearing = (bearing + 360) % 360

            return distance, bearing

        except Exception as e:
            self.logger.error(f"Error in distance calculation: {e}")
            raise

    def getDirectionString(self, bearing: float) -> str:
        """Get direction string based on bearing and configuration"""
        bearing = (bearing + 360) % 360

        if self.config.direction_type == DirectionType.CARDINAL:
            dirs = ["N", "E", "S", "W"]
            idx = int((bearing + 45) % 360 / 90)
            return dirs[idx]

        elif self.config.direction_type == DirectionType.INTERCARDINAL:
            dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
            idx = int((bearing + 22.5) % 360 / 45)
            return dirs[idx]

        else:  # DETAILED
            dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                   "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
            idx = int((bearing + 11.25) % 360 / 22.5)
            return dirs[idx]

    def _find_closest_location(self, event_lat: float, event_lon: float) -> Optional[Tuple[LocationReference, float, str]]:
        """Find closest location to event coordinates"""
        if not self._locations:
            self.logger.error("No locations available")
            return None

        closest = None
        min_distance = float('inf')
        closest_bearing = 0

        self.logger.debug(f"Searching closest location to {event_lat}, {event_lon}")

        for loc in self._locations.values():
            try:
                distance, bearing = self.calculateDistance(
                    loc.lat, loc.lon, event_lat, event_lon)

                if distance > self.config.max_distance:
                    continue

                if distance < min_distance:
                    min_distance = distance
                    closest = loc
                    closest_bearing = bearing

            except Exception as e:
                self.logger.warning(f"Error processing location {loc.name}: {e}")
                continue

        if closest:
            direction = self.getDirectionString(closest_bearing)
            return closest, min_distance, direction

        return None

    def _generate_description(self, location: LocationReference, distance: float, 
                            direction: str) -> str:
        """Generate location description"""
        location_parts = [location.name]
        if self.config.show_state and location.state:
            location_parts.append(location.state)
        if self.config.show_country and location.country:
            location_parts.append(location.country)
        
        base_location = ", ".join(location_parts)
        return self.config.description_pattern.format(
            distance=round(distance),
            direction=direction,
            location=base_location
        )

    def _update_event_description(self, event: DM.Event, description: str) -> bool:
        """Update event description in database"""
        try:
            if self.config.test_mode:
                self.logger.info(f"Test mode: Would update event {event.publicID()} "
                               f"with description: {description}")
                return True

            DM.Notifier.Enable()
            changes_made = False

            # Update both region name and earthquake name
            for desc_type in [DM.REGION_NAME, DM.EARTHQUAKE_NAME]:
                existing_desc = None
                
                # Find existing description
                for i in range(event.eventDescriptionCount()):
                    desc = event.eventDescription(i)
                    if desc.type() == desc_type:
                        existing_desc = desc
                        break

                if existing_desc:
                    if existing_desc.text() != description:
                        existing_desc.setText(description)
                        DM.Notifier.Create(event.publicID(), DM.OP_UPDATE, existing_desc)
                        changes_made = True
                else:
                    new_desc = DM.EventDescription()
                    new_desc.setType(desc_type)
                    new_desc.setText(description)
                    event.add(new_desc)
                    DM.Notifier.Create(event.publicID(), DM.OP_ADD, new_desc)
                    changes_made = True

            if changes_made:
                event.creationInfo().setModificationTime(seiscomp.core.Time.GMT())
                DM.Notifier.Create(event.publicID(), DM.OP_UPDATE, event)
                
                msg = DM.Notifier.GetMessage()
                if msg and not self.connection().send(msg):
                    raise RuntimeError("Failed to send notifier message")
                self.logger.info(f"Updated descriptions for event {event.publicID()}")

            DM.Notifier.Disable()
            return True

        except Exception as e:
            self.logger.error(f"Error updating event description: {e}")
            DM.Notifier.Disable()
            return False

    def updateObject(self, parentID: str, object: seiscomp.core.BaseObject) -> bool:
        """Handle updated events from the messaging system"""
        try:
            event = DM.Event.Cast(object)
            if event:
                self.logger.info(f"Processing updated event: {event.publicID()}")
                return self._process_event(event)
            return True
        except Exception as e:
            self.logger.error(f"Error processing object update: {e}")
            return False

    def addObject(self, parentID: str, object: seiscomp.core.BaseObject) -> bool:
        """Handle new events from the messaging system"""
        try:
            event = DM.Event.Cast(object)
            if event:
                self.logger.info(f"Processing new event: {event.publicID()}")
                return self._process_event(event)
            return True
        except Exception as e:
            self.logger.error(f"Error processing new object: {e}")
            return False

    def _process_event(self, event: DM.Event) -> bool:
        """Process an event and update its location description"""
        try:
            # Load event descriptions
            self.query().loadEventDescriptions(event)
            
            # Check if we have a preferred origin
            if not event.preferredOriginID():
                self.logger.debug(f"No preferred origin for event {event.publicID()}")
                return True
                
            # Load the preferred origin
            origin = DM.Origin.Cast(self.query().loadObject(
                DM.Origin.TypeInfo(), event.preferredOriginID()))
            
            if not origin:
                self.logger.error(f"Could not load preferred origin {event.preferredOriginID()}")
                return False

            # Get coordinates
            try:
                lat = origin.latitude().value()
                lon = origin.longitude().value()
                
                if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                    raise ValueError(f"Invalid coordinates: lat={lat}, lon={lon}")
                    
                self.logger.debug(f"Event coordinates: {lat}, {lon}")
                
            except Exception as e:
                self.logger.error(f"Error reading origin coordinates: {e}")
                return False

            # Find closest location
            result = self.findClosestLocation(lat, lon)
            if not result:
                self.logger.info(f"No nearby locations found for event {event.publicID()}")
                return True

            location, distance, direction = result
            distance_km = round(distance)

            # Generate description
            location_parts = []
            if location.name:
                location_parts.append(location.name)
            if self.config.show_state and location.state:
                location_parts.append(location.state)
            if self.config.show_country and location.country:
                location_parts.append(location.country)
            
            base_location = ", ".join(location_parts)
            description = f"{distance_km} km {direction} of {base_location}"
            
            # Update the event description
            return self.updateEventDescriptions(event, description, description)

        except Exception as e:
            self.logger.error(f"Error processing event {event.publicID()}: {e}")
            return False

    def run(self):
        """Main loop"""
        self.logger.info("Event Location module is running")
        return seiscomp.client.Application.run(self)

def main():
    """Main entry point"""
    try:
        app = EventLocationModule(len(sys.argv), sys.argv)
        returnCode = app()
        if returnCode == 0:
            logging.info("Application completed successfully")
        else:
            logging.error(f"Application failed with return code: {returnCode}")
        return returnCode
    except Exception as e:
        logging.critical(f"Critical application error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())