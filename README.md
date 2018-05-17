# New York Civic Engagement Table Analysis: Run Scripts

## INSTRUCTIONS

### Experiments
1. Install Docker
    - Mac: https://docs.docker.com/docker-for-mac/install
    - Windows: https://docs.docker.com/docker-for-windows/install
2. Run Docker on your computer
3. Add new experiments to the experiments table in the ny database
4. Update the following files in the src/input folder
    - contact_types.csv
      - list all contact types to include in experiment analysis
    - election_mapping.csv
      - year: year of election
      - election: election name in db
      - start: date experiment started
      - end: date experiment ended; normally day of election
    - org_mapping.csv
      - contact_history: org name as it appears in contacthistory table
      - experiments: org name as it appears in experiments table
   
Once above steps are done...

5. Run these commands in a terminal:
    - docker build . -t nycet
    - docker run -t -e PASSWORD=type-password-here nycet

### Competitive Districts

1. Install Docker
    - Mac: https://docs.docker.com/docker-for-mac/install
    - Windows: https://docs.docker.com/docker-for-windows/install

#### To calculate competitive metrics
5. Run these commands in a terminal:
    - docker build . -t nycet-comp
    - docker run -t -e PASSWORD=type-password-here -e FILE_NAME=comp_generator.py nycet-comp

#### To generate demographic metrics
5. Run these commands in a terminal:
    - docker build . -t nycet-demo
    - docker run -t -e PASSWORD=type-password-here -e FILE_NAME=demo_generator.py nycet-demo
