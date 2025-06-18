package utils

import (
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

type ActorInfo struct {
	Name   string
	Movies map[int64]struct{}
}

type ActorsCounter struct {
	Movies map[int64]struct{}
	Actors map[string]*ActorInfo
}

func newActorInfo(name string) *ActorInfo {
	return &ActorInfo{
		Name:   name,
		Movies: make(map[int64]struct{}),
	}
}

func NewActorCounter() *ActorsCounter {
	return &ActorsCounter{
		Movies: make(map[int64]struct{}),
		Actors: make(map[string]*ActorInfo),
	}
}

func (counter *ActorsCounter) AppendMovie(movie *protopb.MovieSanit) {
	counter.Movies[int64(movie.GetId())] = struct{}{}
}

func (counter *ActorsCounter) Count(credit *protopb.CreditSanit) {
	movieID := credit.GetId()

	for index := 0; index < len(credit.GetCastNames()); index++ {
		profilePath := credit.ProfilePaths[index]
		name := credit.CastNames[index]

		actor, exists := counter.Actors[profilePath]
		if !exists {
			actor = newActorInfo(name)
			counter.Actors[profilePath] = actor
		}

		actor.Movies[movieID] = struct{}{}
	}
}

func (counter *ActorsCounter) GetActor(profilePath string, clientID string, messageId int64, sourceId string) *protopb.Actor {
	actor, exists := counter.Actors[profilePath]
	if !exists {
		return nil
	}

	validMovieCount := int64(0)
	for movieID := range actor.Movies {
		_, movieExists := counter.Movies[movieID]
		if movieExists {
			validMovieCount++
		}
	}

	return &protopb.Actor{
		Name:        proto.String(actor.Name),
		CountMovies: proto.Int64(validMovieCount),
		ProfilePath: proto.String(profilePath),
		ClientId:    proto.String(clientID),
		MessageId:   proto.Int64(messageId),
		SourceId:    proto.String(sourceId),
	}
}

func CreateActorEof(clientID string, messageId int64, sourceId string) *protopb.Actor {
	return &protopb.Actor{
		Name:        proto.String("DUMMY"),
		CountMovies: proto.Int64(0),
		ProfilePath: proto.String("DUMMY"),
		Eof:         proto.Bool(true),
		ClientId:    proto.String(clientID),
		MessageId:   proto.Int64(messageId),
		SourceId:    proto.String(sourceId),
	}
}
