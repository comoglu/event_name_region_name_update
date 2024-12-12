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

def setup_logging(debug_mode: bool = False) -> logging.Logger:
    logger = logging.getLogger("EventNaming")
    logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Rotating file handler (10MB max size, keep 5 backup files)
    file_handler = RotatingFileHandler(
        'event_naming.log', maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

class DirectionType(Enum):
    CARDINAL = "cardinal"
    INTERCARDINAL = "intercardinal"
    DETAILED = "detailed"

class EventNamingConfig:
    """Configuration container for event naming parameters"""
    def __init__(self):
        self.max_distance = 1000.0
        self.min_population = 50000
        self.direction_type = DirectionType.DETAILED
        self.description_pattern = "{poi} {dist}km {dir}"
        self.regions_enabled = True
        self.debug_mode = False
        self.show_state = True
        self.show_country = True
        self.update_region = False
        self.test = False

    @classmethod
    def from_config_file(cls, config_file: str) -> "EventNamingConfig":
        config = cls()
        try:
            with open(config_file, 'r') as f:
                # Implement config file parsing if needed
                pass
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
        return config

class EventNaming(seiscomp.client.Application):
    def __init__(self, argc: int, argv: List[str]):
        super().__init__(argc, argv)
        self.setMessagingEnabled(True)
        self.setDatabaseEnabled(True, True)
        self.setDaemonEnabled(False)
        self.setPrimaryMessagingGroup("EVENT")

        self.config = EventNamingConfig()
        self.logger = setup_logging()
        self._locations: Dict[str, LocationReference] = {}
        self.test = False
        self.locations_file = None

    def createCommandLineDescription(self):
        self.commandline().addGroup("Event")
        self.commandline().addStringOption("Event", "eventID,E", "Event ID to process")
        self.commandline().addStringOption("Event", "locations-file,L", 
                                         "CSV file with reference locations")
        self.commandline().addStringOption("Event", "direction-type,D",
                                         "Direction type (cardinal, intercardinal, detailed)")
        self.commandline().addDoubleOption("Event", "max-distance,M",
                                         "Maximum distance to consider (km)")
        self.commandline().addOption("Event", "update-region,U", 
                                   "Update region name with generated description")
        self.commandline().addOption("Event", "test,T", "Test mode - no database updates")
        self.commandline().addOption("Event", "verbose,v", "Verbose output")
        return True

    def validateParameters(self):
        if not super(EventNaming, self).validateParameters():
            return False

        try:
            if not self.commandline().hasOption("eventID"):
                self.logger.error("No event ID specified, use --eventID")
                return False

            if not self.commandline().hasOption("locations-file"):
                self.logger.error("No locations file specified, use --locations-file")
                return False

            # Handle direction type
            if self.commandline().hasOption("direction-type"):
                direction_str = self.commandline().optionString("direction-type").upper()
                try:
                    self.config.direction_type = DirectionType[direction_str]
                except KeyError:
                    self.logger.error(f"Invalid direction type: {direction_str}")
                    return False

            # Handle max distance
            if self.commandline().hasOption("max-distance"):
                self.config.max_distance = self.commandline().optionDouble("max-distance")
                if self.config.max_distance <= 0:
                    self.logger.error("Max distance must be positive")
                    return False

            # Handle update-region option
            self.config.update_region = self.commandline().hasOption("update-region")
            
            # Set debug mode
            if self.commandline().hasOption("verbose"):
                self.logger.setLevel(logging.DEBUG)
                self.config.debug_mode = True

            # Store locations file path
            self.locations_file = self.commandline().optionString("locations-file")

            # Set test mode
            self.test = self.commandline().hasOption("test")
            if self.test:
                self.logger.info("Running in test mode - no database updates will be sent")

            return True

        except Exception as e:
            self.logger.error(f"Parameter validation failed: {e}")
            return False

    def loadLocations(self) -> bool:
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

    def getDirectionString(self, bearing: float) -> str:
        """Enhanced direction string generator with multiple granularity levels"""
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

    def findClosestLocation(self, event_lat: float, event_lon: float) -> Optional[Tuple[LocationReference, float, str]]:
        """Find closest location with enhanced filtering and validation"""
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

        self.logger.warning("No location found within maximum distance")
        return None

    def updateEventDescriptions(self, event: DM.Event, region_name: str, location_name: str) -> bool:
        """Update event descriptions in the database"""
        try:
            self.logger.debug(f"Updating descriptions - Region: {region_name}, Location: {location_name}")
            
            # Enable notifier before making changes
            DM.Notifier.Enable()
            
            # Track if we made any changes
            changes_made = False
            
            # Update region name if flag is set
            if self.config.update_region:
                region_desc = None
                # Find existing region description
                for i in range(event.eventDescriptionCount()):
                    desc = event.eventDescription(i)
                    if desc.type() == DM.REGION_NAME:
                        region_desc = desc
                        break
                
                if region_desc:
                    if region_desc.text() != region_name:
                        self.logger.debug(f"Updating existing region name to: {region_name}")
                        region_desc.setText(region_name)
                        event.creationInfo().setModificationTime(seiscomp.core.Time.GMT())
                        DM.Notifier.Create("EventParameters", DM.OP_UPDATE, event)
                        DM.Notifier.Create(event, DM.OP_UPDATE, region_desc)
                        changes_made = True
                else:
                    self.logger.debug(f"Creating new region name: {region_name}")
                    region_desc = DM.EventDescription()
                    region_desc.setType(DM.REGION_NAME)
                    region_desc.setText(region_name)
                    event.add(region_desc)
                    event.creationInfo().setModificationTime(seiscomp.core.Time.GMT())
                    DM.Notifier.Create("EventParameters", DM.OP_UPDATE, event)
                    DM.Notifier.Create(event, DM.OP_ADD, region_desc)
                    changes_made = True

            # Update earthquake name (keeping existing working code)
            location_desc = None
            for i in range(event.eventDescriptionCount()):
                desc = event.eventDescription(i)
                if desc.type() == DM.EARTHQUAKE_NAME:
                    location_desc = desc
                    break
            
            if location_desc:
                if location_desc.text() != location_name:
                    self.logger.debug(f"Updating existing earthquake name to: {location_name}")
                    location_desc.setText(location_name)
                    DM.Notifier.Create(event.publicID(), DM.OP_UPDATE, location_desc)
                    changes_made = True
            else:
                self.logger.debug(f"Creating new earthquake name: {location_name}")
                location_desc = DM.EventDescription()
                location_desc.setType(DM.EARTHQUAKE_NAME)
                location_desc.setText(location_name)
                event.add(location_desc)
                DM.Notifier.Create(event.publicID(), DM.OP_ADD, location_desc)
                changes_made = True

            # If changes were made and not in test mode, send the updates
            if changes_made and not self.test:
                msg = DM.Notifier.GetMessage()
                if msg:
                    success = self.connection().send(msg)
                    if not success:
                        raise RuntimeError("Failed to send notifier message")
                    self.logger.info("Successfully sent database updates")
                else:
                    self.logger.warning("No changes to send")
            elif self.test:
                self.logger.info("Test mode - skipping database updates")
            else:
                self.logger.info("No changes needed")

            DM.Notifier.Disable()
            return True

        except Exception as e:
            self.logger.error(f"Error updating event descriptions: {e}")
            DM.Notifier.Disable()
            return False

    def addEventComment(self, event: DM.Event, comment: str, id: str = "EventNaming") -> bool:
        """Add a comment to the event"""
        try:
            commentObj = DM.Comment()
            commentObj.setId(id)
            commentObj.setText(comment)
            event.add(commentObj)

            if not self.test:
                DM.Notifier.Enable()
                DM.Notifier.Create(event.publicID(), DM.OP_UPDATE, commentObj)
                msg = DM.Notifier.GetMessage()
                if msg:
                    self.connection().send(msg)
                DM.Notifier.Disable()

            self.logger.debug(f"Added event comment: {comment}")
            return True

        except Exception as e:
            self.logger.error(f"Error adding event comment: {e}")
            DM.Notifier.Disable()
            return False

    def run(self):
        """Main processing function"""
        try:
            # Load event
            event_id = self.commandline().optionString("eventID")
            event = DM.Event.Cast(self.query().loadObject(DM.Event.TypeInfo(), event_id))
            
            if not event:
                raise ValueError(f"Event {event_id} not found or invalid")
            
            # Load event descriptions
            self.query().loadEventDescriptions(event)
            
            # Get preferred origin
            if not event.preferredOriginID():
                raise ValueError("No preferred origin set for event")
                
            origin = DM.Origin.Cast(self.query().loadObject(
                DM.Origin.TypeInfo(), event.preferredOriginID()))
                
            if not origin:
                raise ValueError(f"Preferred origin {event.preferredOriginID()} not found")

            # Extract coordinates
            try:
                lat = origin.latitude().value()
                lon = origin.longitude().value()
                self.logger.info(f"Event coordinates: {lat}, {lon}")

                if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                    raise ValueError(f"Invalid coordinates: lat={lat}, lon={lon}")

            except ValueError as e:
                self.logger.error(f"Error reading origin coordinates: {e}")
                return False

            # Load locations if not already loaded
            if not self._locations and not self.loadLocations():
                return False

            # Find the closest location
            result = self.findClosestLocation(lat, lon)
            if not result:
                self.logger.info("No location found within maximum distance. Taking no action.")
                return True  # Return True as this is an expected condition

            location, distance, direction = result
            distance_km = round(distance)

            # Format location name
            location_parts = []
            if location.name:
                location_parts.append(location.name)
            if self.config.show_state and location.state:
                location_parts.append(location.state)
            if self.config.show_country and location.country:
                location_parts.append(location.country)

            base_location = ", ".join(location_parts)
            description = f"{distance_km} km {direction} of {base_location}"

            self.logger.info(f"Generated description: {description}")

            # Update both region name and earthquake name to the same value
            if self.updateEventDescriptions(event, description, description):
                self.logger.info("Successfully updated event descriptions")

                # Add additional information as comment if in debug mode
                if self.config.debug_mode:
                    comment = (f"Location details: Distance={distance_km}km, "
                             f"Direction={direction}, Coordinates={lat:.3f},{lon:.3f}")
                    self.addEventComment(event, comment)

                return True
            else:
                self.logger.error("Failed to update event descriptions")
                return False

        except Exception as e:
            self.logger.error(f"Error in main processing: {e}")
            return False


def main():
    """Main entry point with enhanced error handling"""
    try:
        app = EventNaming(len(sys.argv), sys.argv)
        logger = logging.getLogger("EventNaming")
        logger.info("Starting Event Naming application")

        returnCode = app()
        if returnCode == 0:
            logger.info("Application completed successfully")
        else:
            logger.error(f"Application failed with return code: {returnCode}")
        return returnCode

    except Exception as e:
        logging.critical(f"Critical application error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())