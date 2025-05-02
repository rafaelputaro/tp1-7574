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
		Adult:               proto.Bool(false),
		BelongsToCollection: proto.String("{'id': 10194, 'name': 'Toy Story Collection', 'poster_path': '/7G9915LfUQ2lVfwMEEhDsn3kT4B.jpg', 'backdrop_path': '/9FBwqcd9IRruEDUrTdcaafOMKUq.jpg'}"),
		Budget:              proto.Int32(30000000),
		Genres:              proto.String("[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"),
		Homepage:            proto.String("http://toystory.disney.com/toy-story"),
		Id:                  proto.Int32(862),
		ImdbId:              proto.String("tt0114709"),
		OriginalLanguage:    proto.String("en"),
		OriginalTitle:       proto.String("Toy Story"),
		Overview:            proto.String("Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences."),
		Popularity:          proto.Float32(21.946943),
		PosterPath:          proto.String("/rhIRbceoE9lR4veEXuwCC2wARtG.jpg"),
		ProductionCompanies: proto.String("[{'name': 'Pixar Animation Studios', 'id': 3}]"),
		ProductionCountries: proto.String("[{'iso_3166_1': 'US', 'name': 'United States of America'}]"),
		ReleaseDate:         proto.String("1995-10-30"),
		Revenue:             proto.Float64(373554033),
		Runtime:             proto.Float64(81.0),
		SpokenLanguages:     proto.String("[{'iso_639_1': 'en', 'name': 'English'}]"),
		Status:              proto.String("Released"),
		Tagline:             proto.String(""),
		Title:               proto.String("Toy Story"),
		Video:               proto.Bool(false),
		VoteAverage:         proto.Float64(7.7),
		VoteCount:           proto.Int32(5415),
		ClientId:            proto.String("1"),
		Eof:                 proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.Movie{}))
}

func TestCredit(t *testing.T) {
	message := &protopb.Credit{
		Cast: proto.String("[{'cast_id': 14, 'character': 'Woody (voice)', 'credit_id': '52fe4284c3a36847f8024f95', 'gender': 2, 'id': 31, 'name': 'Tom Hanks', 'order': 0, 'profile_path': '/pQFoyx7rp09CJTAb932F2g8Nlho.jpg'}, {'cast_id': 15, 'character': 'Buzz Lightyear (voice)', 'credit_id': '52fe4284c3a36847f8024f99', 'gender': 2, 'id': 12898, 'name': 'Tim Allen', 'order': 1, 'profile_path': '/uX2xVf6pMmPepxnvFWyBtjexzgY.jpg'}, {'cast_id': 16, 'character': 'Mr. Potato Head (voice)', 'credit_id': '52fe4284c3a36847f8024f9d', 'gender': 2, 'id': 7167, 'name': 'Don Rickles', 'order': 2, 'profile_path': '/h5BcaDMPRVLHLDzbQavec4xfSdt.jpg'}, {'cast_id': 17, 'character': 'Slinky Dog (voice)', 'credit_id': '52fe4284c3a36847f8024fa1', 'gender': 2, 'id': 12899, 'name': 'Jim Varney', 'order': 3, 'profile_path': '/eIo2jVVXYgjDtaHoF19Ll9vtW7h.jpg'}, {'cast_id': 18, 'character': 'Rex (voice)', 'credit_id': '52fe4284c3a36847f8024fa5', 'gender': 2, 'id': 12900, 'name': 'Wallace Shawn', 'order': 4, 'profile_path': '/oGE6JqPP2xH4tNORKNqxbNPYi7u.jpg'}, {'cast_id': 19, 'character': 'Hamm (voice)', 'credit_id': '52fe4284c3a36847f8024fa9', 'gender': 2, 'id': 7907, 'name': 'John Ratzenberger', 'order': 5, 'profile_path': '/yGechiKWL6TJDfVE2KPSJYqdMsY.jpg'}, {'cast_id': 20, 'character': 'Bo Peep (voice)', 'credit_id': '52fe4284c3a36847f8024fad', 'gender': 1, 'id': 8873, 'name': 'Annie Potts', 'order': 6, 'profile_path': '/eryXT84RL41jHSJcMy4kS3u9y6w.jpg'}, {'cast_id': 26, 'character': 'Andy (voice)', 'credit_id': '52fe4284c3a36847f8024fc1', 'gender': 0, 'id': 1116442, 'name': 'John Morris', 'order': 7, 'profile_path': '/vYGyvK4LzeaUCoNSHtsuqJUY15M.jpg'}, {'cast_id': 22, 'character': 'Sid (voice)', 'credit_id': '52fe4284c3a36847f8024fb1', 'gender': 2, 'id': 12901, 'name': 'Erik von Detten', 'order': 8, 'profile_path': '/twnF1ZaJ1FUNUuo6xLXwcxjayBE.jpg'}, {'cast_id': 23, 'character': 'Mrs. Davis (voice)', 'credit_id': '52fe4284c3a36847f8024fb5', 'gender': 1, 'id': 12133, 'name': 'Laurie Metcalf', 'order': 9, 'profile_path': '/unMMIT60eoBM2sN2nyR7EZ2BvvD.jpg'}, {'cast_id': 24, 'character': 'Sergeant (voice)', 'credit_id': '52fe4284c3a36847f8024fb9', 'gender': 2, 'id': 8655, 'name': 'R. Lee Ermey', 'order': 10, 'profile_path': '/r8GBqFBjypLUP9VVqDqfZ7wYbSs.jpg'}, {'cast_id': 25, 'character': 'Hannah (voice)', 'credit_id': '52fe4284c3a36847f8024fbd', 'gender': 1, 'id': 12903, 'name': 'Sarah Freeman', 'order': 11, 'profile_path': None}, {'cast_id': 27, 'character': 'TV Announcer (voice)', 'credit_id': '52fe4284c3a36847f8024fc5', 'gender': 2, 'id': 37221, 'name': 'Penn Jillette', 'order': 12, 'profile_path': '/zmAaXUdx12NRsssgHbk1T31j2x9.jpg'}]"),
		Crew: proto.String(`[{'credit_id': '52fe4284c3a36847f8024f49', 'department': 'Directing', 'gender': 2, 'id': 7879, 'job': 'Director', 'name': 'John Lasseter', 'profile_path': '/7EdqiNbr4FRjIhKHyPPdFfEEEFG.jpg'}, {'credit_id': '52fe4284c3a36847f8024f4f', 'department': 'Writing', 'gender': 2, 'id': 12891, 'job': 'Screenplay', 'name': 'Joss Whedon', 'profile_path': '/dTiVsuaTVTeGmvkhcyJvKp2A5kr.jpg'}, {'credit_id': '52fe4284c3a36847f8024f55', 'department': 'Writing', 'gender': 2, 'id': 7, 'job': 'Screenplay', 'name': 'Andrew Stanton', 'profile_path': '/pvQWsu0qc8JFQhMVJkTHuexUAa1.jpg'}, {'credit_id': '52fe4284c3a36847f8024f5b', 'department': 'Writing', 'gender': 2, 'id': 12892, 'job': 'Screenplay', 'name': 'Joel Cohen', 'profile_path': '/dAubAiZcvKFbboWlj7oXOkZnTSu.jpg'}, {'credit_id': '52fe4284c3a36847f8024f61', 'department': 'Writing', 'gender': 0, 'id': 12893, 'job': 'Screenplay', 'name': 'Alec Sokolow', 'profile_path': '/v79vlRYi94BZUQnkkyznbGUZLjT.jpg'}, {'credit_id': '52fe4284c3a36847f8024f67', 'department': 'Production', 'gender': 1, 'id': 12894, 'job': 'Producer', 'name': 'Bonnie Arnold', 'profile_path': None}, {'credit_id': '52fe4284c3a36847f8024f6d', 'department': 'Production', 'gender': 0, 'id': 12895, 'job': 'Executive Producer', 'name': 'Ed Catmull', 'profile_path': None}, {'credit_id': '52fe4284c3a36847f8024f73', 'department': 'Production', 'gender': 2, 'id': 12896, 'job': 'Producer', 'name': 'Ralph Guggenheim', 'profile_path': None}, {'credit_id': '52fe4284c3a36847f8024f79', 'department': 'Production', 'gender': 2, 'id': 12897, 'job': 'Executive Producer', 'name': 'Steve Jobs', 'profile_path': '/mOMP3SwD5qWQSR0ldCIByd3guTV.jpg'}, {'credit_id': '52fe4284c3a36847f8024f8b', 'department': 'Editing', 'gender': 2, 'id': 8, 'job': 'Editor', 'name': 'Lee Unkrich', 'profile_path': '/bdTCCXjgOV3YyaNmLGYGOxFQMOc.jpg'}, {'credit_id': '52fe4284c3a36847f8024f91', 'department': 'Art', 'gender': 2, 'id': 7883, 'job': 'Art Direction', 'name': 'Ralph Eggleston', 'profile_path': '/uUfcGKDsKO1aROMpXRs67Hn6RvR.jpg'}, {'credit_id': '598331bf925141421201044b', 'department': 'Editing', 'gender': 2, 'id': 1168870, 'job': 'Editor', 'name': 'Robert Gordon', 'profile_path': None}, {'credit_id': '5892168cc3a36809660095f9', 'department': 'Sound', 'gender': 0, 'id': 1552883, 'job': 'Foley Editor', 'name': 'Mary Helen Leasman', 'profile_path': None}, {'credit_id': '5531824d9251415289000945', 'department': 'Visual Effects', 'gender': 0, 'id': 1453514, 'job': 'Animation', 'name': 'Kim Blanchette', 'profile_path': None}, {'credit_id': '589215969251412dcb009bf6', 'department': 'Sound', 'gender': 0, 'id': 1414182, 'job': 'ADR Editor', 'name': 'Marilyn McCoppen', 'profile_path': None}, {'credit_id': '589217099251412dc500a018', 'department': 'Sound', 'gender': 2, 'id': 7885, 'job': 'Orchestrator', 'name': 'Randy Newman', 'profile_path': '/w0JzfoiM25nrnxYOzosPHRq6mlE.jpg'}, {'credit_id': '5693e6b29251417b0e0000e3', 'department': 'Editing', 'gender': 0, 'id': 1429549, 'job': 'Color Timer', 'name': 'Dale E. Grahn', 'profile_path': None}, {'credit_id': '572e2522c3a36869e6001a9c', 'department': 'Visual Effects', 'gender': 0, 'id': 7949, 'job': 'CG Painter', 'name': 'Robin Cooper', 'profile_path': None}, {'credit_id': '574f12309251415ca1000012', 'department': 'Writing', 'gender': 2, 'id': 7879, 'job': 'Original Story', 'name': 'John Lasseter', 'profile_path': '/7EdqiNbr4FRjIhKHyPPdFfEEEFG.jpg'}, {'credit_id': '574f1240c3a3682e7300001c', 'department': 'Writing', 'gender': 2, 'id': 12890, 'job': 'Original Story', 'name': 'Pete Docter', 'profile_path': '/r6ngPgnReA3RHmKjmSoVsc6Awjp.jpg'}, {'credit_id': '574f12519251415c92000015', 'department': 'Writing', 'gender': 0, 'id': 7911, 'job': 'Original Story', 'name': 'Joe Ranft', 'profile_path': '/f1BoWC2JbCcfP1e5hKfGsxkHzVU.jpg'}, {'credit_id': '574f12cec3a3682e82000022', 'department': 'Crew', 'gender': 0, 'id': 1629419, 'job': 'Post Production Supervisor', 'name': 'Patsy Bouge', 'profile_path': None}, {'credit_id': '574f14f19251415ca1000082', 'department': 'Art', 'gender': 0, 'id': 7961, 'job': 'Sculptor', 'name': 'Norm DeCarlo', 'profile_path': None}, {'credit_id': '5751ae4bc3a3683772002b7f', 'department': 'Visual Effects', 'gender': 2, 'id': 12905, 'job': 'Animation Director', 'name': 'Ash Brannon', 'profile_path': '/6ueWgPEEBHvS3De2BHYQnYjRTig.jpg'}, {'credit_id': '5891edbe9251412dc5007cd6', 'department': 'Sound', 'gender': 2, 'id': 7885, 'job': 'Music', 'name': 'Randy Newman', 'profile_path': '/w0JzfoiM25nrnxYOzosPHRq6mlE.jpg'}, {'credit_id': '589213d39251412dc8009832', 'department': 'Directing', 'gender': 0, 'id': 1748707, 'job': 'Layout', 'name': 'Roman Figun', 'profile_path': None}, {'credit_id': '5892173dc3a3680968009351', 'department': 'Sound', 'gender': 2, 'id': 4949, 'job': 'Orchestrator', 'name': 'Don Davis', 'profile_path': None}, {'credit_id': '589217cec3a3686b0a0052ba', 'department': 'Sound', 'gender': 0, 'id': 1372885, 'job': 'Music Editor', 'name': 'James Flamberg', 'profile_path': None}, {'credit_id': '58921831c3a3686348004a64', 'department': 'Editing', 'gender': 0, 'id': 1739962, 'job': 'Negative Cutter', 'name': 'Mary Beth Smith', 'profile_path': None}, {'credit_id': '58921838c3a36809700096c0', 'department': 'Editing', 'gender': 0, 'id': 1748513, 'job': 'Negative Cutter', 'name': 'Rick Mackay', 'profile_path': None}, {'credit_id': '589218429251412dd1009d1b', 'department': 'Art', 'gender': 0, 'id': 1458006, 'job': 'Title Designer', 'name': 'Susan Bradley', 'profile_path': None}, {'credit_id': '5891ed99c3a3680966007670', 'department': 'Crew', 'gender': 0, 'id': 1748557, 'job': 'Supervising Technical Director', 'name': 'William Reeves', 'profile_path': None}, {'credit_id': '5891edcec3a3686b0a002eb2', 'department': 'Sound', 'gender': 2, 'id': 7885, 'job': 'Songs', 'name': 'Randy Newman', 'profile_path': '/w0JzfoiM25nrnxYOzosPHRq6mlE.jpg'}, {'credit_id': '5891edf9c3a36809700075e6', 'department': 'Writing', 'gender': 2, 'id': 7, 'job': 'Original Story', 'name': 'Andrew Stanton', 'profile_path': '/pvQWsu0qc8JFQhMVJkTHuexUAa1.jpg'}, {'credit_id': '58920f0b9251412dd7009104', 'department': 'Crew', 'gender': 2, 'id': 12890, 'job': 'Supervising Animator', 'name': 'Pete Docter', 'profile_path': '/r6ngPgnReA3RHmKjmSoVsc6Awjp.jpg'}, {'credit_id': '58920f1fc3a3680977009021', 'department': 'Sound', 'gender': 2, 'id': 2216, 'job': 'Sound Designer', 'name': 'Gary Rydstrom', 'profile_path': '/jZpr1nVfO7lldWI0YtmP1FGw7Rj.jpg'}, {'credit_id': '58920f389251412dd700912d', 'department': 'Production', 'gender': 0, 'id': 12909, 'job': 'Production Supervisor', 'name': 'Karen Robert Jackson', 'profile_path': None}, {'credit_id': '58920fbd9251412dcb00969c', 'department': 'Crew', 'gender': 0, 'id': 953331, 'job': 'Executive Music Producer', 'name': 'Chris Montan', 'profile_path': None}, {'credit_id': '589210069251412dd7009219', 'department': 'Visual Effects', 'gender': 0, 'id': 7893, 'job': 'Animation Director', 'name': 'Rich Quade', 'profile_path': None}, {'credit_id': '589210329251412dcd00943b', 'department': 'Visual Effects', 'gender': 0, 'id': 8025, 'job': 'Animation', 'name': 'Michael Berenstein', 'profile_path': None}, {'credit_id': '5892103bc3a368096a009180', 'department': 'Visual Effects', 'gender': 0, 'id': 78009, 'job': 'Animation', 'name': 'Colin Brady', 'profile_path': None}, {'credit_id': '5892105dc3a3680968008db2', 'department': '
Visual Effects', 'gender': 0, 'id': 1748682, 'job': 'Animation', 'name': 'Davey Crockett Feiten', 'profile_path': None}, {'credit_id': '589210669251412dcd009466', 'department': 'Visual Effects', 'gender': 0, 'id': 1454030, 'job': 'Animation', 'name': 'Angie Glocka', 'profile_path': None}, {'credit_id': '5892107c9251412dd1009613', 'department': 'Visual Effects', 'gender': 0, 'id': 1748683, 'job': 'Animation', 'name': 'Rex Grignon', 'profile_path': None}, {'credit_id': '5892108ac3a3680973008d3f', 'department': 'Visual Effects', 'gender': 0, 'id': 1748684, 'job': 'Animation', 'name': 'Tom K. Gurney', 'profile_path': None}, {'credit_id': '58921093c3a3686348004477', 'department': 'Visual Effects', 'gender': 2, 'id': 8029, 'job': 'Animation', 'name': 'Jimmy Hayward', 'profile_path': '/lTDRpudEY7BDwTefXbXzMlmb0ui.jpg'}, {'credit_id': '5892109b9251412dcd0094b0', 'department': 'Visual Effects', 'gender': 0, 'id': 1426773, 'job': 'Animation', 'name': 'Hal T. Hickel', 'profile_path': None}, {'credit_id': '589210a29251412dc5009a29', 'department': 'Visual Effects', 'gender': 0, 'id': 8035, 'job': 'Animation', 'name': 'Karen Kiser', 'profile_path': None}, {'credit_id': '589210ccc3a3680977009191', 'department': 'Visual Effects', 'gender': 0, 'id': 1748688, 'job': 'Animation', 'name': 'Anthony B. LaMolinara', 'profile_path': None}, {'credit_id': '589210d7c3a3686b0a004c1f', 'department': 'Visual Effects', 'gender': 0, 'id': 587314, 'job': 'Animation', 'name': 'Guionne Leroy', 'profile_path': None}, {'credit_id': '589210e1c3a36809770091a7', 'department': 'Visual Effects', 'gender': 2, 'id': 7918, 'job': 'Animation', 'name': 'Bud Luckey', 'profile_path': '/pcCh7G19FKMNijmPQg1PMH1btic.jpg'}, {'credit_id': '589210ee9251412dc200978a', 'department': 'Visual Effects', 'gender': 0, 'id': 1748689, 'job': 'Animation', 'name': 'Les Major', 'profile_path': None}, {'credit_id': '589210fa9251412dc8009595', 'department': 'Visual Effects', 'gender': 2, 'id': 7892, 'job': 'Animation', 'name': 'Glenn McQueen', 'profile_path': None}, {'credit_id': '589211029251412dc8009598', 'department': 'Visual Effects', 'gender': 0, 'id': 555795, 'job': 'Animation', 'name': 'Mark Oftedal', 'profile_path': None}, {'credit_id': '5892110b9251412dc800959d', 'department': 'Visual Effects', 'gender': 2, 'id': 7882, 'job': 'Animation', 'name': 'Jeff Pidgeon', 'profile_path': '/yLddkg5HcgbJg00cS13GVBnP0HY.jpg'}, {'credit_id': '58921113c3a36863480044e4', 'department': 'Visual Effects', 'gender': 0, 'id': 8017, 'job': 'Animation', 'name': 'Jeff Pratt', 'profile_path': None}, {'credit_id': '5892111c9251412dcb0097e9', 'department': 'Visual Effects', 'gender': 0, 'id': 1184140, 'job': 'Animation', 'name': 'Steve Rabatich', 'profile_path': None}, {'credit_id': '58921123c3a36809700090f6', 'department': 'Visual Effects', 'gender': 0, 'id': 8049, 'job': 'Animation', 'name': 'Roger Rose', 'profile_path': None}, {'credit_id': '5892112b9251412dcb0097fb', 'department': 'Visual Effects', 'gender': 0, 'id': 1509559, 'job': 'Animation', 'name': 'Steve Segal', 'profile_path': None}, {'credit_id': '589211349251412dc80095c3', 'department': 'Visual Effects', 'gender': 0, 'id': 1748691, 'job': 'Animation', 'name': 'Doug Sheppeck', 'profile_path': None}, {'credit_id': '5892113cc3a3680970009106', 'department': 'Visual Effects', 'gender': 0, 'id': 8050, 'job': 'Animation', 'name': 'Alan Sperling', 'profile_path': None}, {'credit_id': '58921148c3a3686b0a004c99', 'department': 'Visual Effects', 'gender': 0, 'id': 8010, 'job': 'Animation', 'name': 'Doug Sweetland', 'profile_path': None}, {'credit_id': '58921150c3a3680966009125', 'department': 'Visual Effects', 'gender': 0, 'id': 8044, 'job': 'Animation', 'name': 'David Tart', 'profile_path': None}, {'credit_id': '589211629251412dc5009b00', 'department': 'Visual Effects', 'gender': 0, 'id': 1454034, 'job': 'Animation', 'name': 'Ken Willard', 'profile_path': None}, {'credit_id': '589211c1c3a3686b0a004d28', 'department': 'Visual Effects', 'gender': 0, 'id': 7887, 'job': 'Visual Effects Supervisor', 'name': 'Thomas Porter', 'profile_path': None}, {'credit_id': '589211d4c3a3680968008ed9', 'department': 'Visual Effects', 'gender': 0, 'id': 1406878, 'job': 'Visual Effects', 'name': 'Mark Thomas Henne', 'profile_path': None}, {'credit_id': '589211f59251412dd4008e65', 'department': 'Visual Effects', 'gender': 0, 'id': 1748698, 'job': 'Visual Effects', 'name': 'Oren Jacob', 'profile_path': None}, {'credit_id': '58921242c3a368096a00939b', 'department': 'Visual Effects', 'gender': 0, 'id': 1748699, 'job': 'Visual Effects', 'name': 'Darwyn Peachey', 'profile_path': None}, {'credit_id': '5892124b9251412dc5009bd2', 'department': 'Visual Effects', 'gender': 0, 'id': 1748701, 'job': 'Visual Effects', 'name': 'Mitch Prater', 'profile_path': None}, {'credit_id': '58921264c3a3686b0a004dbf', 'department': 'Visual Effects', 'gender': 0, 'id': 1748703, 'job': 'Visual Effects', 'name': 'Brian M. Rosen', 'profile_path': None}, {'credit_id': '589212709251412dcd009676', 'department': 'Lighting', 'gender': 1, 'id': 12912, 'job': 'Lighting Supervisor', 'name': 'Sharon Calahan', 'profile_path': None}, {'credit_id': '5892127fc3a3686b0a004de5', 'department': 'Lighting', 'gender': 0, 'id': 7899, 'job': 'Lighting Supervisor', 'name': 'Galyn Susman', 'profile_path': None}, {'credit_id': '589212cdc3a3680970009268', 'department': 'Visual Effects', 'gender': 0, 'id': 12915, 'job': 'CG Painter', 'name': 'William Cone', 'profile_path': None}, {'credit_id': '5892130f9251412dc8009791', 'department': 'Art', 'gender': 0, 'id': 1748705, 'job': 'Sculptor', 'name': 'Shelley Daniels Lekven', 'profile_path': None}, {'credit_id': '5892131c9251412dd4008f4c', 'department': 'Visual Effects', 'gender': 2, 'id': 7889, 'job': 'Character Designer', 'name': 'Bob Pauley', 'profile_path': None}, {'credit_id': '589213249251412dd100987b', 'department': 'Visual Effects', 'gender': 2, 'id': 7918, 'job': 'Character Designer', 'name': 'Bud Luckey', 'profile_path': '/pcCh7G19FKMNijmPQg1PMH1btic.jpg'}, {'credit_id': '5892132b9251412dc80097b1', 'department': 'Visual Effects', 'gender': 2, 'id': 7, 'job': 'Character Designer', 'name': 'Andrew Stanton', 'profile_path': '/pvQWsu0qc8JFQhMVJkTHuexUAa1.jpg'}, {'credit_id': '58921332c3a368634800467b', 'department': 'Visual Effects', 'gender': 0, 'id': 12915, 'job': 'Character Designer', 'name': 'William Cone', 'profile_path': None}, {'credit_id': '5892135f9251412dd4008f90', 'department': 'Visual Effects', 'gender': 0, 'id': 1748706, 'job': 'Character Designer', 'name': 'Steve Johnson', 'profile_path': None}, {'credit_id': '58921384c3a3680973008fd4', 'department': 'Visual Effects', 'gender': 0, 'id': 1176752, 'job': 'Character Designer', 'name': 'Dan Haskett', 'profile_path': None}, {'credit_id': '5892138e9251412dc20099fc', 'department': 'Visual Effects', 'gender': 0, 'id': 1088034, 'job': 'Character Designer', 'name': 'Tom Holloway', 'profile_path': '/a0r0T2usTBpgMI5aZbRBDW1fTl8.jpg'}, {'credit_id': '58921395c3a368097700942f', 'department': 'Visual Effects', 'gender': 0, 'id': 1447465, 'job': 'Character Designer', 'name': 'Jean Gillmore', 'profile_path': None}, {'credit_id': '589213e2c3a3680973009026', 'department': 'Directing', 'gender': 0, 'id': 1748709, 'job': 'Layout', 'name': 'Desirée Mourad', 'profile_path': None}, {'credit_id': '589214099251412dc5009d57', 'department': 'Art', 'gender': 0, 'id': 1748710, 'job': 'Set Dresser', 'name': ""Kelly O'Connell"", 'profile_path': None}, {'credit_id': '58921411c3a3686b0a004f70', 'department': 'Art', 'gender': 0, 'id': 1443471, 'job': 'Set Dresser', 'name': 'Sonoko Konishi', 'profile_path': None}, {'credit_id': '58921434c3a368096a00956e', 'department': 'Art', 'gender': 0, 'id': 1748711, 'job': 'Set Dresser', 'name': 'Ann M. Rockwell', 'profile_path': None}, {'credit_id': '5892144ac3a36809680090de', 'department': 'Editing', 'gender': 0, 'id': 1748712, 'job': 'Editorial Manager', 'name': 'Julie M. McDonald', 'profile_path': None}, {'credit_id': '58921479c3a368096800910f', 'department': 'Editing', 'gender': 0, 'id': 1589729, 'job': 'Assistant Editor', 'name': 'Robin Lee', 'profile_path': None}, {'credit_id': '5892148b9251412dd10099cc', 'department': 'Editing', 'gender': 0, 'id': 1748716, 'job': 'Assistant Editor', 'name': 'Tom Freeman', 'profile_path': None}, {'credit_id': '589214959251412dcb009b1f', 'department': 'Editing', 'gender': 0, 'id': 1748717, 'job': 'Assistant Editor', 'name': 'Ada Cochavi', 'profile_path': None}, {'credit_id': '5892149ec3a3686348004798', 'department': 'Editing', 'gender': 0, 'id': 1336438, 'job': 'Assistant Editor', 'name': 'Dana Mulligan', 'profile_path': None}, {'credit_id': '589214adc3a368096a0095db', 'department': 'Editing', 'gender': 0, 'id': 1748718, 'job': 'Editorial Coordinator', 'name': 'Deirdre Morrison', 'profile_path': None}, {'credit_id': '589214c7c3a368097700952b', 'department': 'Production', 'gender': 0, 'id': 1748719, 'job': 'Production Coordinator', 'name': 'Lori Lombardo', 'profile_path': None}, {'credit_id': '589214cec3a368096a009603', 'department': 'Production', 'gender': 0, 'id': 1748720, 'job': 'Production Coordinator', 'name': 'Ellen Devine', 'profile_path': None}, {'credit_id': '589214e39251412dc8009904', 'department': 'Crew', 'gender': 0, 'id': 1468014, 'job': 'Unit Publicist', 'name': 'Lauren Beth Strogoff', 'profile_path': None}, {'credit_id': '58921544c3a3686b0a00507d', 'department': 'Sound', 'gender': 2, 'id': 2216, 'job': 'Sound Re-Recording Mixer', 'name': 'Gary Rydstrom', 'profile_path': '/jZpr1nVfO7lldWI0YtmP1FGw7Rj.jpg'}, {'credit_id': '5892154c9251412dd1009a56', 'department': 'Sound', 'gender': 0, 'id': 1425978, 'job': 'Sound Re-Recording Mixer', 'name': 'Gary Summers', 'profile_path': None}, {'credit_id': '58921555c3a36809680091bd', 'department': 'Sound', 'gender': 2, 'id': 8276, 'job': 'Supervising Sound Editor', 'name': 'Tim Holland', 'profile_path': None}, {'credit_id': '589215c39251412dcb009c12', 'department': 'Sound', 'gender': 0, 'id': 7069, 'job': 'Sound
Effects Editor', 'name': 'Pat Jackson', 'profile_path': None}, {'credit_id': '58921698c3a368096a009788', 'department': 'Crew', 'gender': 2, 'id': 15894, 'job': 'Sound Design Assistant', 'name': 'Tom Myers', 'profile_path': None}, {'credit_id': '589216a89251412dc2009ca4', 'department': 'Sound', 'gender': 0, 'id': 1414177, 'job': 'Assistant Sound Editor', 'name': 'J.R. Grubbs', 'profile_path': None}, {'credit_id': '589216c19251412dc2009cb9', 'department': 'Sound', 'gender': 1, 'id': 1748724, 'job': 'Assistant Sound Editor', 'name': 'Susan Sanford', 'profile_path': None}, {'credit_id': '589216ccc3a3680973009274', 'department': 'Sound', 'gender': 0, 'id': 1748725, 'job': 'Assistant Sound Editor', 'name': 'Susan Popovic', 'profile_path': None}, {'credit_id': '589216d79251412dc8009aa0', 'department': 'Sound', 'gender': 0, 'id': 8067, 'job': 'Assistant Sound Editor', 'name': 'Dan Engstrom', 'profile_path': None}, {'credit_id': '589216e49251412dcd009a4f', 'department': 'Production', 'gender': 1, 'id': 7902, 'job': 'Casting Consultant', 'name': 'Ruth Lambert', 'profile_path': None}, {'credit_id': '589216f39251412dc2009cf3', 'department': 'Production', 'gender': 0, 'id': 84493, 'job': 'ADR Voice Casting', 'name': 'Mickie McGowan', 'profile_path': '/k7TjJBfINsg8vLQxJwos6XObAD6.jpg'}]`),
		Id:       proto.Int64(862),
		ClientId: proto.String("1"),
		Eof:      proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.Credit{}))
}

func TestRating(t *testing.T) {
	message := &protopb.Rating{
		UserId:    proto.Int64(1),
		MovieId:   proto.Int64(31),
		Rating:    proto.Float32(2.5),
		Timestamp: proto.Int64(1260759144),
		ClientId:  proto.String("1"),
		Eof:       proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.Rating{}))
}

func TestMovieSanit(t *testing.T) {
	message := &protopb.MovieSanit{
		Budget:              proto.Int32(30000000),
		Genres:              []string{"Animation", "Comedy", "Family"},
		Id:                  proto.Int32(862),
		Overview:            proto.String("Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences."),
		ProductionCountries: []string{"US"},
		ReleaseYear:         proto.Uint32(1995),
		Revenue:             proto.Float64(373554033),
		Title:               proto.String("Toy Story"),
		ClientId:            proto.String("1"),
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
		Eof:          proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.CreditSanit{}))
}

func TestRatingSanit(t *testing.T) {
	message := &protopb.RatingSanit{
		MovieId:  proto.Int64(31),
		Rating:   proto.Float32(2.5),
		ClientId: proto.String("1"),
		Eof:      proto.Bool(false),
	}
	data, err := proto.Marshal(message)
	checkMarshal(t, data, err)
	checkUnmarshal(t, proto.Unmarshal(data, &protopb.RatingSanit{}))
}

func TestEof(t *testing.T) {
	actor := protoUtils.CreateEofActor("0")
	credit := protoUtils.CreateEofCredit("0")
	creditSanit := protoUtils.CreateEofCreditSanit("0")
	metrics := protoUtils.CreateEofMetrics("0")
	movie := protoUtils.CreateEofMovie("0")
	movieSanit := protoUtils.CreateEofMovieSanit("0")
	rating := protoUtils.CreateEofRating("0")
	ratingSanit := protoUtils.CreateEofRatingSanit("0")
	revOveBud := protoUtils.CreateEofRevenueOverBudget("0")
	top10 := protoUtils.CreateEofTop10("0")
	top5Country := protoUtils.CreateEofTop5Country("0")
	topAndBottomRatAvg := protoUtils.CreateEofTopAndBottomRatingAvg("0")
	test := actor.GetEof() && credit.GetEof() && creditSanit.GetEof() && metrics.GetEof() &&
		movie.GetEof() && movieSanit.GetEof() && rating.GetEof() && ratingSanit.GetEof() &&
		rating.GetEof() && revOveBud.GetEof() && top10.GetEof() && top5Country.GetEof() &&
		topAndBottomRatAvg.GetEof()
	if !test {
		t.Fatal("Error on create Eof")
	}
}
