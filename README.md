# SeisComP Event Name and Region Update

## Overview

A Python application for SeisComP that automatically updates event descriptions and region names based on nearby geographical locations. The application calculates the distance and direction to the nearest significant location and updates both the earthquake name and region name fields in the SeisComP database.

## Features

- Automatic generation of event descriptions based on nearest cities/locations
- Configurable maximum distance for location matching
- Support for different direction formats (cardinal, intercardinal, detailed)
- Customizable population threshold for reference locations
- Option to include state and country information
- Detailed logging with debug mode
- Test mode for dry runs without database updates

## Prerequisites

- Python 3.7+
- SeisComP installation with Python bindings
- Access to a SeisComP database with write permissions

## Installation

1. Clone the repository:
```bash
git clone https://github.com/comoglu/event_name_region_name_update.git
cd event_name_region_name_update
```

2. Ensure SeisComP Python bindings are in your Python path

## Configuration

### Location Reference File

Create a CSV file with reference locations in the following format:

```csv
name,state,country,latitude,longitude,population
Perth,WA,Australia,-31.9523,115.8613,2000000
```

Required columns:
- `name`: City/location name
- `state`: State/province
- `country`: Country name
- `latitude`: Decimal degrees (-90 to 90)
- `longitude`: Decimal degrees (-180 to 180)
- `population`: Population count (used for filtering)

## Usage

Basic usage:
```bash
seiscomp-python event_name_region_name_update.py -E <event_id> -L <locations_file>
```

All options:
```bash
seiscomp-python event_name_region_name_update.py [options]

Options:
  -E, --eventID       Event ID to process
  -L, --locations-file Path to CSV file with reference locations
  -D, --direction-type Direction type (cardinal, intercardinal, detailed)
  -M, --max-distance  Maximum distance to consider (km)
  -U, --update-region Update region name (optional)
  -T, --test         Test mode - no database updates
  -v, --verbose      Enable verbose logging
```

### Examples

Update event with ID "ga2024ylnfds":
```bash
seiscomp-python event_name_region_name_update.py -E ga2024ylnfds -L locations.csv -U --verbose
```

Test mode (no database updates):
```bash
seiscomp-python event_name_region_name_update.py -E ga2024ylnfds -L locations.csv -U --test
```

## Output Format

The application generates descriptions in the format:
```
[distance] km [direction] of [city], [state], [country]
```

Example:
```
643 km ESE of Perth, WA, Australia
```

## Behavior

1. If a location is found within the maximum distance:
   - Updates both earthquake name and region name (if -U flag is used)
   - Formats description with distance and direction

2. If no location is found within the maximum distance:
   - Takes no action
   - Logs the situation if in verbose mode

## Logging

- Default log file: `event_naming.log`
- Use `--verbose` for detailed logging
- Logs rotate at 10MB with 5 backup files

## Error Handling

The application includes comprehensive error handling for:
- Invalid coordinates
- Missing locations file
- Database connection issues
- Invalid event IDs
- Malformed location data

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

For issues and feature requests, please use the [GitHub issue tracker](https://github.com/comoglu/event_name_region_name_update/issues).

## Acknowledgments

- Built for use with SeisComP
- Based on SeisComP's Python API