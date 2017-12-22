# Gremlin Marvel Dataset

## The Application

Simple Java application that performs the following operation on the Marvel Dataset (CSV format):

* Unzip the files
* Create a TinkerGraph in-memory database
* Load the vertices: Heroes and Comics
* Load the edges connecting Heroes to Comics
* Load the social network between the Heroes
* Save the graph to a GraphML file

## The Dataset

The dataset consist of three files in CSV format that specify:

* All the vertices: Heroes and Comics
* The edges from Heroes to Comics. Each row represents the apparition of a Hero in a Comic number
* A social network between the Heroes. Each row represents Heroes appeared together.

## Notes

You can swap the TinkerGraph for any compatible database