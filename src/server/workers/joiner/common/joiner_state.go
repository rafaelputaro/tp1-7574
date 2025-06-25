package common

import (
	"tp1/helpers/state"
	"tp1/helpers/window"
	"tp1/rabbitmq"
	"tp1/server/workers/joiner/common/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

const MESSAGE_FAILED_TO_CREATE_STATE_HELPER string = "Failed to create state helper"
const MESSAGE_UNABLE_TO_SAVE_STATE string = "Unable to save state"

type AckArgs struct {
	msg amqp.Delivery
}

type ClientStatesCreditsInternal struct {
	ClientStates map[string]*utils.ClientStateCredits
}

type ActorsCounterDB struct {
	Movies map[int64]struct{}
	Actors map[string]utils.ActorInfo
}

type ClientStateCreditsDB struct {
	Counter   ActorsCounterDB
	MovieEOF  bool
	CreditEOF bool
}

type JoinerCreditsState struct {
	ClientStates map[string]ClientStateCreditsDB
}

type JoinerCreditsUpdateArgs struct {
	MessageId          int64
	ClientId           string
	SourceId           string
	CreditMovieId      int64
	CreditCastNames    []string
	CreditProfilePaths []string
	MovieId            int32
	IsCredit           bool
	EOF                bool
}

type ClientStatesRatingsInternal struct {
	ClientStates map[string]*utils.ClientStateRatings
}

type RatingTotalizerDB struct {
	Movies     map[int64]utils.MovieInfo
	SeenMovies map[int64]struct{}
}

type ClientStateRatingsDB struct {
	Totalizer RatingTotalizerDB
	MovieEOF  bool
	RatingEOF bool
}

type JoinerRatingsState struct {
	ClientStates map[string]ClientStateRatingsDB
}

type JoinerRatingsUpdateArgs struct {
	MessageId     int64
	ClientId      string
	SourceId      string
	RatingMovieId int64
	Rating        float32
	MovieId       int32
	MovieTitle    string
	IsRating      bool
	EOF           bool
}

func SendAck(args AckArgs) error {
	err := rabbitmq.SingleAck(args.msg)
	if err != nil {
		Log.Fatalf("failed to ack message: %v", err)
		return err
	}
	return nil
}

// Create a state from file or from scratch.
func (joiner *Joiner) CreateJoinerCreditsState() *ClientStatesCreditsInternal {
	joinerStateDB, _ := state.GetLastValidState(joiner.StateHelperCredits)
	var joinerState = &ClientStatesCreditsInternal{
		ClientStates: make(map[string]*utils.ClientStateCredits),
	}
	if joinerStateDB != nil {
		for keyDB, state := range joinerStateDB.ClientStates {
			counter := utils.ActorsCounter{
				Movies: state.Counter.Movies,
				Actors: make(map[string]*utils.ActorInfo),
			}
			for keyAct, actInfo := range state.Counter.Actors {
				counter.Actors[keyAct] = &actInfo
			}
			clientStateCredits := utils.ClientStateCredits{
				Counter:   &counter,
				MovieEOF:  state.MovieEOF,
				CreditEOF: state.CreditEOF,
			}
			joinerState.ClientStates[keyDB] = &clientStateCredits
		}
	}
	return joinerState
}

// Create a state from file or from scratch.
func (joiner *Joiner) CreateJoinerRatingsState() *ClientStatesRatingsInternal {
	joinerStateDB, _ := state.GetLastValidState(joiner.StateHelperRatings)
	var joinerState = &ClientStatesRatingsInternal{
		ClientStates: make(map[string]*utils.ClientStateRatings),
	}
	if joinerStateDB != nil {
		for keyDB, state := range joinerStateDB.ClientStates {
			totalizer := utils.RatingTotalizer{
				Movies:     make(map[int64]*utils.MovieInfo),
				SeenMovies: state.Totalizer.SeenMovies,
			}
			for keyMov, movInfo := range state.Totalizer.Movies {
				totalizer.Movies[keyMov] = &movInfo
			}
			clientStateRatings := utils.ClientStateRatings{
				Totalizer: &totalizer,
				MovieEOF:  state.MovieEOF,
				RatingEOF: state.RatingEOF,
			}

			joinerState.ClientStates[keyDB] = &clientStateRatings
		}
	}
	return joinerState
}

// Return the state helpers and the window
func (joiner *Joiner) InitStateHelperCredits(exchange string) {
	stateHelper := state.NewStateHelper[JoinerCreditsState, JoinerCreditsUpdateArgs, AckArgs](
		joiner.Config.ID,
		joiner.Config.JoinerType+"_credits",
		exchange,
		UpdateCredits)
	if stateHelper == nil {
		joiner.Log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
		return
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	joiner.StateHelperCredits = stateHelper
	joiner.MessagesWindow = &messageWindow
}

// Return the state helpers and the window
func (joiner *Joiner) InitStateHelperRatings(exchange string) {
	stateHelper := state.NewStateHelper[JoinerRatingsState, JoinerRatingsUpdateArgs, AckArgs](
		joiner.Config.ID,
		joiner.Config.JoinerType+"_ratings",
		exchange,
		UpdateRatings)
	if stateHelper == nil {
		joiner.Log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
		return
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	joiner.StateHelperRatings = stateHelper
	joiner.MessagesWindow = &messageWindow
}

// Updates the joiner status and refresh the window
func UpdateCredits(joinerState *JoinerCreditsState, messageWindow *window.MessageWindow, updateArgs *JoinerCreditsUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.SourceId, updateArgs.MessageId)
	state := joinerState.ClientStates[updateArgs.ClientId]
	if updateArgs.IsCredit {
		if updateArgs.EOF {
			state.CreditEOF = true
		} else {
			state.Counter.Count(updateArgs.CreditMovieId, updateArgs.CreditCastNames, updateArgs.CreditProfilePaths)
		}
		joinerState.ClientStates[updateArgs.ClientId] = state
		return
	}
	if updateArgs.EOF {
		state.MovieEOF = true
	} else {
		state.Counter.AppendMovie(updateArgs.MovieId)
	}
	joinerState.ClientStates[updateArgs.ClientId] = state
}

// Updates the joiner status and refresh the window
func UpdateRatings(joinerState *JoinerRatingsState, messageWindow *window.MessageWindow, updateArgs *JoinerRatingsUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.SourceId, updateArgs.MessageId)
	state := joinerState.ClientStates[updateArgs.ClientId]
	if updateArgs.IsRating {
		if updateArgs.EOF {
			state.RatingEOF = true
		} else {
			state.Totalizer.Sum(updateArgs.RatingMovieId, updateArgs.Rating)
		}
		joinerState.ClientStates[updateArgs.ClientId] = state
		return
	}
	if updateArgs.EOF {
		state.MovieEOF = true
	} else {
		state.Totalizer.AppendMovie(updateArgs.MovieId, updateArgs.MovieTitle)
	}
	joinerState.ClientStates[updateArgs.ClientId] = state
}

// Refresh the window, save the state and send the ack
func (joiner *Joiner) SaveCreditsState(
	joinerStateInternal *ClientStatesCreditsInternal,
	msg amqp.Delivery,
	clientId string,
	sourceId string,
	creditMovieId int64,
	creditCastNames []string,
	creditProfilePaths []string,
	movieId int32,
	isCredit bool,
	eof bool,
	messageId int64) error {
	// update window
	joiner.MessagesWindow.AddMessage(clientId, sourceId, messageId)
	// save state
	var joinerStateDB = JoinerCreditsState{
		ClientStates: make(map[string]ClientStateCreditsDB),
	}
	for keyInt, state := range joinerStateInternal.ClientStates {
		counter := ActorsCounterDB{
			Movies: state.Counter.Movies,
			Actors: make(map[string]utils.ActorInfo),
		}
		for keyAct, actInfo := range state.Counter.Actors {
			counter.Actors[keyAct] = *actInfo
		}
		clientStateCredits := ClientStateCreditsDB{
			Counter:   counter,
			MovieEOF:  state.MovieEOF,
			CreditEOF: state.CreditEOF,
		}
		joinerStateDB.ClientStates[keyInt] = clientStateCredits
	}
	err := state.SaveState(
		joiner.StateHelperCredits,
		joinerStateDB,
		&AckArgs{
			msg: msg,
		},
		SendAck,
		*joiner.MessagesWindow,
		JoinerCreditsUpdateArgs{
			ClientId:           clientId,
			MessageId:          messageId,
			SourceId:           sourceId,
			CreditMovieId:      creditMovieId,
			CreditCastNames:    creditCastNames,
			CreditProfilePaths: creditProfilePaths,
			MovieId:            movieId,
			IsCredit:           isCredit,
			EOF:                eof,
		})
	if err != nil {
		joiner.Log.Fatalf(MESSAGE_UNABLE_TO_SAVE_STATE)
		return err
	}
	return nil
}

// Refresh the window, save the state and send the ack
func (joiner *Joiner) SaveRatingsState(
	joinerStateInternal *ClientStatesRatingsInternal,
	msg amqp.Delivery,
	clientId string,
	sourceId string,
	ratingMovieId int64,
	rating float32,
	movieId int32,
	movieTitle string,
	isRating bool,
	eof bool,
	messageId int64) error {
	// update window
	joiner.MessagesWindow.AddMessage(clientId, sourceId, messageId)
	// save state

	var joinerStateDB = JoinerRatingsState{
		ClientStates: make(map[string]ClientStateRatingsDB),
	}
	if joinerStateInternal != nil {
		for keyDB, state := range joinerStateInternal.ClientStates {
			totalizer := RatingTotalizerDB{
				Movies:     make(map[int64]utils.MovieInfo),
				SeenMovies: state.Totalizer.SeenMovies,
			}
			for keyMov, movInfo := range state.Totalizer.Movies {
				totalizer.Movies[keyMov] = *movInfo
			}
			clientStateRatings := ClientStateRatingsDB{
				Totalizer: totalizer,
				MovieEOF:  state.MovieEOF,
				RatingEOF: state.RatingEOF,
			}

			joinerStateDB.ClientStates[keyDB] = clientStateRatings
		}
	}
	err := state.SaveState(
		joiner.StateHelperRatings,
		joinerStateDB,
		&AckArgs{
			msg: msg,
		},
		SendAck,
		*joiner.MessagesWindow,
		JoinerRatingsUpdateArgs{
			ClientId:      clientId,
			MessageId:     messageId,
			SourceId:      sourceId,
			RatingMovieId: ratingMovieId,
			Rating:        rating,
			MovieId:       movieId,
			MovieTitle:    movieTitle,
			IsRating:      isRating,
			EOF:           eof,
		})
	if err != nil {
		joiner.Log.Fatalf(MESSAGE_UNABLE_TO_SAVE_STATE)
		return err
	}
	return nil
}

func (counter *ActorsCounterDB) Count(creditMovieId int64, creditCastNames []string, creditProfilePaths []string) {
	for index := 0; index < len(creditCastNames); index++ {
		profilePath := creditProfilePaths[index]
		name := creditCastNames[index]
		actor, exists := counter.Actors[profilePath]
		if !exists {
			actor = utils.ActorInfo{
				Name:   name,
				Movies: make(map[int64]struct{}),
			}
			counter.Actors[profilePath] = actor
		}
		actor.Movies[creditMovieId] = struct{}{}
	}
}

func (counter *ActorsCounterDB) AppendMovie(movieId int32) {
	counter.Movies[int64(movieId)] = struct{}{}
}

func (totalizer *RatingTotalizerDB) Sum(ratingMovieId int64, rating float32) {
	if _, exists := totalizer.Movies[ratingMovieId]; !exists {
		totalizer.Movies[ratingMovieId] = utils.MovieInfo{
			Title:     "",
			RatingSum: 0,
			Count:     0,
			RatingAvg: 0,
		}
	}
	newTotalizerMovie := totalizer.Movies[ratingMovieId]
	newTotalizerMovie.RatingAvg = newTotalizerMovie.RatingAvg + float64(rating)
	newTotalizerMovie.Count = newTotalizerMovie.Count + 1
	totalizer.Movies[ratingMovieId] = newTotalizerMovie
}

func (totalizer *RatingTotalizerDB) AppendMovie(movieId int32, movieTitle string) {
	movieID := int64(movieId)
	totalizer.SeenMovies[movieID] = struct{}{}
	if _, exists := totalizer.Movies[movieID]; !exists {
		totalizer.Movies[movieID] = utils.MovieInfo{
			Title:     movieTitle,
			RatingSum: 0,
			Count:     0,
			RatingAvg: 0,
		}
	} else {
		totaMovie := totalizer.Movies[movieID]
		totaMovie.Title = movieTitle
		totalizer.Movies[movieID] = totaMovie
	}
}

func GenerateSourceIdMovies(sourceId string) string {
	return "movies_" + sourceId
}

func (joiner *Joiner) DisposeStateHelpers() {
	if joiner.StateHelperCredits != nil {
		joiner.StateHelperCredits.Dispose(SendAck)
	}
	if joiner.StateHelperRatings != nil {
		joiner.StateHelperRatings.Dispose(SendAck)
	}
}
