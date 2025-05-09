package utils

import (
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

type ActorData struct {
	Name        string
	CountMovies int64
}

type ActorsCounter struct {
	Actors map[string]ActorData // key: profile path
	Movies map[int64]bool       // key: movie id (Works as set)
}

func newActorData(name string, count int64) *ActorData {
	return &ActorData{
		Name:        name,
		CountMovies: count,
	}
}

func NewActorCounter() *ActorsCounter {
	return &ActorsCounter{
		Actors: make(map[string]ActorData),
		Movies: make(map[int64]bool),
	}
}

// append a movie
func (counter *ActorsCounter) AppendMovie(movie *protopb.MovieSanit) {
	counter.Movies[int64(*movie.Id)] = true
}

// update count for the entire cast if credit math with a movie
func (counter *ActorsCounter) Count(credit *protopb.CreditSanit) {
	_, exists := counter.Movies[*credit.Id]
	if exists {
		for index := range len(credit.CastNames) {
			counter.countActor(credit.CastNames[index], credit.ProfilePaths[index])
		}
	}
	// TODO: Si no llego todavia la movie? Habria que agregar algo igual
}

// update count for an actor
func (counter *ActorsCounter) countActor(name string, profilePath string) {
	founded, exists := counter.Actors[profilePath]
	if exists {
		// update
		newData := *newActorData(name, founded.CountMovies+1)
		counter.Actors[profilePath] = newData
	} else {
		// insert
		newData := *newActorData(name, 1)
		counter.Actors[profilePath] = newData
	}
}

// profilePath is valid
func (counter *ActorsCounter) GetActor(profilePath string, clientID string) *protopb.Actor {
	founded := counter.Actors[profilePath]
	return &protopb.Actor{
		Name:        proto.String(founded.Name),
		CountMovies: proto.Int64(founded.CountMovies),
		ProfilePath: proto.String(profilePath),
		ClientId:    proto.String(clientID),
	}
}

func CreateActorEof(clientID string) *protopb.Actor {
	return &protopb.Actor{
		Name:        proto.String("DUMMY"),
		CountMovies: proto.Int64(0),
		ProfilePath: proto.String("DUMMY"),
		Eof:         proto.Bool(true),
		ClientId:    proto.String(clientID),
	}
}
