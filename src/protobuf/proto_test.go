package proto

import (
	"testing"
	"tp1/protobuf/protopb"
	protoUtils "tp1/protobuf/utils"

	"google.golang.org/protobuf/proto"
)

func checkMarshal(t *testing.T, data []byte, err error) {
	if err != nil {
		t.Fatal("Error serializing: ", err)
	}
	lenData := len(data)
	if lenData < 0 {
		t.Fatal("Length error while serializing: ", lenData)
	}
}

func checkUnmarshal(t *testing.T, err error) {
	if err != nil {
		t.Fatal("Error deserializing: ", err)
	}
}

func TestMovie(t *testing.T) {
	message := &protopb.Movie{
		Budget:              proto.Int64(30000000),
		Genres:              proto.String("[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"),
		Id:                  proto.Int32(862),
		Overview:            proto.String("Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences."),
		ProductionCountries: proto.String("[{'iso_3166_1': 'US', 'name': 'United States of America'}]"),
		ReleaseDate:         proto.String("1995-10-30"),
		Revenue:             proto.Float64(373554033),
		SpokenLanguages:     proto.String("[{'iso_639_1': 'en', 'name': 'English'}]"),
		Title:               proto.String("Toy Story"),
		ClientId:            proto.String("1"),
		MessageId:           proto.Int64(1),
		Eof:                 proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.Movie{}))
}

func TestCredit(t *testing.T) {
	message := &protopb.Credit{
		Cast:      proto.String("[{'cast_id': 14, 'character': 'Woody (voice)', 'credit_id': '52fe4284c3a36847f8024f95', 'gender': 2, 'id': 31, 'name': 'Tom Hanks', 'order': 0, 'profile_path': '/pQFoyx7rp09CJTAb932F2g8Nlho.jpg'}, {'cast_id': 15, 'character': 'Buzz Lightyear (voice)', 'credit_id': '52fe4284c3a36847f8024f99', 'gender': 2, 'id': 12898, 'name': 'Tim Allen', 'order': 1, 'profile_path': '/uX2xVf6pMmPepxnvFWyBtjexzgY.jpg'}, {'cast_id': 16, 'character': 'Mr. Potato Head (voice)', 'credit_id': '52fe4284c3a36847f8024f9d', 'gender': 2, 'id': 7167, 'name': 'Don Rickles', 'order': 2, 'profile_path': '/h5BcaDMPRVLHLDzbQavec4xfSdt.jpg'}, {'cast_id': 17, 'character': 'Slinky Dog (voice)', 'credit_id': '52fe4284c3a36847f8024fa1', 'gender': 2, 'id': 12899, 'name': 'Jim Varney', 'order': 3, 'profile_path': '/eIo2jVVXYgjDtaHoF19Ll9vtW7h.jpg'}, {'cast_id': 18, 'character': 'Rex (voice)', 'credit_id': '52fe4284c3a36847f8024fa5', 'gender': 2, 'id': 12900, 'name': 'Wallace Shawn', 'order': 4, 'profile_path': '/oGE6JqPP2xH4tNORKNqxbNPYi7u.jpg'}, {'cast_id': 19, 'character': 'Hamm (voice)', 'credit_id': '52fe4284c3a36847f8024fa9', 'gender': 2, 'id': 7907, 'name': 'John Ratzenberger', 'order': 5, 'profile_path': '/yGechiKWL6TJDfVE2KPSJYqdMsY.jpg'}, {'cast_id': 20, 'character': 'Bo Peep (voice)', 'credit_id': '52fe4284c3a36847f8024fad', 'gender': 1, 'id': 8873, 'name': 'Annie Potts', 'order': 6, 'profile_path': '/eryXT84RL41jHSJcMy4kS3u9y6w.jpg'}, {'cast_id': 26, 'character': 'Andy (voice)', 'credit_id': '52fe4284c3a36847f8024fc1', 'gender': 0, 'id': 1116442, 'name': 'John Morris', 'order': 7, 'profile_path': '/vYGyvK4LzeaUCoNSHtsuqJUY15M.jpg'}, {'cast_id': 22, 'character': 'Sid (voice)', 'credit_id': '52fe4284c3a36847f8024fb1', 'gender': 2, 'id': 12901, 'name': 'Erik von Detten', 'order': 8, 'profile_path': '/twnF1ZaJ1FUNUuo6xLXwcxjayBE.jpg'}, {'cast_id': 23, 'character': 'Mrs. Davis (voice)', 'credit_id': '52fe4284c3a36847f8024fb5', 'gender': 1, 'id': 12133, 'name': 'Laurie Metcalf', 'order': 9, 'profile_path': '/unMMIT60eoBM2sN2nyR7EZ2BvvD.jpg'}, {'cast_id': 24, 'character': 'Sergeant (voice)', 'credit_id': '52fe4284c3a36847f8024fb9', 'gender': 2, 'id': 8655, 'name': 'R. Lee Ermey', 'order': 10, 'profile_path': '/r8GBqFBjypLUP9VVqDqfZ7wYbSs.jpg'}, {'cast_id': 25, 'character': 'Hannah (voice)', 'credit_id': '52fe4284c3a36847f8024fbd', 'gender': 1, 'id': 12903, 'name': 'Sarah Freeman', 'order': 11, 'profile_path': None}, {'cast_id': 27, 'character': 'TV Announcer (voice)', 'credit_id': '52fe4284c3a36847f8024fc5', 'gender': 2, 'id': 37221, 'name': 'Penn Jillette', 'order': 12, 'profile_path': '/zmAaXUdx12NRsssgHbk1T31j2x9.jpg'}]"),
		Id:        proto.Int64(862),
		ClientId:  proto.String("1"),
		MessageId: proto.Int64(1),
		Eof:       proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.Credit{}))
}

func TestRating(t *testing.T) {
	message := &protopb.Rating{
		MovieId:   proto.Int64(31),
		Rating:    proto.Float32(2.5),
		Timestamp: proto.Int64(1260759144),
		ClientId:  proto.String("1"),
		MessageId: proto.Int64(1),
		Eof:       proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.Rating{}))
}

func TestMovieSanit(t *testing.T) {
	message := &protopb.MovieSanit{
		Budget:              proto.Int64(30000000),
		Genres:              []string{"Animation", "Comedy", "Family"},
		Id:                  proto.Int32(862),
		Overview:            proto.String("Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences."),
		ProductionCountries: []string{"US"},
		ReleaseYear:         proto.Uint32(1995),
		Revenue:             proto.Float64(373554033),
		Title:               proto.String("Toy Story"),
		ClientId:            proto.String("1"),
		MessageId:           proto.Int64(1),
		SourceId:            proto.String(""),
		Eof:                 proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.MovieSanit{}))
}

func TestCreditSanit(t *testing.T) {
	message := &protopb.CreditSanit{
		CastNames:    []string{"Tom Hanks", "Tim Allen", "Don Rickles", "Jim Varney"},
		ProfilePaths: []string{"/pQFoyx7rp09CJTAb932F2g8Nlho.jpg", "/uX2xVf6pMmPepxnvFWyBtjexzgY.jpg", "/h5BcaDMPRVLHLDzbQavec4xfSdt.jpg", "/eIo2jVVXYgjDtaHoF19Ll9vtW7h.jpg"},
		Id:           proto.Int64(862),
		ClientId:     proto.String("1"),
		MessageId:    proto.Int64(1),
		Eof:          proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.CreditSanit{}))
}

func TestRatingSanit(t *testing.T) {
	message := &protopb.RatingSanit{
		MovieId:   proto.Int64(31),
		Rating:    proto.Float32(2.5),
		ClientId:  proto.String("1"),
		MessageId: proto.Int64(1),
		Eof:       proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.RatingSanit{}))
}

func TestEof(t *testing.T) {
	actor := protoUtils.CreateEofActor("0", 1, "")
	credit := protoUtils.CreateEofCredit("0", 1)
	creditSanit := protoUtils.CreateEofCreditSanit("0", 1)
	metrics := protoUtils.CreateEofMetrics("0", 1)
	movie := protoUtils.CreateEofMovie("0", 1)
	movieSanit := protoUtils.CreateEofMovieSanit("0", 1, "source_1")
	rating := protoUtils.CreateEofRating("0", 1)
	ratingSanit := protoUtils.CreateEofRatingSanit("0", 1)
	revOveBud := protoUtils.CreateEofRevenueOverBudget("0", 1)
	top10 := protoUtils.CreateEofTop10("0", 1)
	top5Country := protoUtils.CreateEofTop5Country("0", 1)
	topAndBottomRatAvg := protoUtils.CreateEofTopAndBottomRatingAvg("0", 1, "")
	test := actor.GetEof() && credit.GetEof() && creditSanit.GetEof() && metrics.GetEof() &&
		movie.GetEof() && movieSanit.GetEof() && rating.GetEof() && ratingSanit.GetEof() &&
		rating.GetEof() && revOveBud.GetEof() && top10.GetEof() && top5Country.GetEof() &&
		topAndBottomRatAvg.GetEof()
	if !test {
		t.Fatal("Error on create Eof")
	}
}
