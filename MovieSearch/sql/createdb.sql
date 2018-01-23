CREATE TABLE Movies
(
	mid VARCHAR(5),
	country VARCHAR(50),
	title VARCHAR(150),
	year NUMBER,
	rtAllCriticsRating NUMBER,
	rtAllCriticsNumReviews NUMBER,
	rtTopCriticsRating NUMBER,
	rtTopCriticsNumReviews NUMBER,
	rtAudienceRating NUMBER,
	rtAudienceNumRatings NUMBER,
	PRIMARY KEY( mid )
);

CREATE TABLE Movie_genres
(
	mid VARCHAR(5),
	genre VARCHAR(25),
	PRIMARY KEY (mid, genre),
	FOREIGN KEY (mid) REFERENCES Movies(mid)
);

CREATE TABLE Movie_location
(
	mid VARCHAR(5),
	location VARCHAR(50),
	PRIMARY KEY (mid, location),
	FOREIGN KEY (mid) REFERENCES Movies(mid)
);

CREATE TABLE Directors
(
	did VARCHAR(40),
	dname VARCHAR(40),
	mid VARCHAR(5),
	PRIMARY KEY( did, mid ),
	FOREIGN KEY (mid) REFERENCES Movies(mid)
);

CREATE TABLE Actors
( 
	aid VARCHAR(50),
	aname VARCHAR(50),
	mid VARCHAR(5),
	PRIMARY KEY( aid, mid),
	FOREIGN KEY (mid) REFERENCES Movies(mid)
);

CREATE TABLE Tags
(
	tid VARCHAR(5),
	value VARCHAR(50),
	PRIMARY KEY(tid)
);

CREATE TABLE Movie_tags
(
	mid VARCHAR(5),
	tid VARCHAR(5),
	tagWeight NUMBER,
	PRIMARY KEY (mid, tid),
	FOREIGN KEY (mid) REFERENCES Movies(mid),
	FOREIGN KEY (tid) REFERENCES Tags(tid)
);

CREATE TABLE User_ratedmovies
(
	userid VARCHAR(5),
	mid VARCHAR(5),
	rating NUMBER,
	YYYYMMdd DATE,
	PRIMARY KEY (userid, mid),
	FOREIGN KEY (mid) REFERENCES Movies(mid)
);

CREATE  INDEX user_mid ON 
User_ratedmovies (mid);

CREATE  INDEX actor_mid ON 
Actors (mid);

CREATE  INDEX director_mid ON 
Directors (mid);

CREATE  INDEX director_name ON 
Directors (dname);

CREATE  INDEX actor_name ON 
Actors (aname);

CREATE INDEX movie_country ON 
Movies (country);