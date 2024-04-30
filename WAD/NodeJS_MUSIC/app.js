const express = require("express");
const bodyparser = require("body-parser");
const Songs = require("./models");
const mongoose = require("mongoose");
const path = require("path");

const app = express();
app.set("view engine", "ejs");
app.use(bodyparser.urlencoded({
  extended: true
}));
app.use(bodyparser.json());

//DB Connection
mongoose
  .connect("mongodb://127.0.0.1:27017/music")
  .then(()=> {
    console.log("DB Connected Successfully!");
  })
  .catch((e)=>{
    console.log("Error in DB connection: ", e);
    process.exit();
  });


//Using CSS
app.use("/css", express.static(path.resolve(__dirname, "static/css")));

//Using ejs
app.get("/", (req,res) => {
  res.render("index", {music:null});
});

//Insert Data
app.post("/addSong", async (req, res) => {
    const song = new Songs(req.body);
    await song
    .save()
    .then((item) => {
      res.redirect("/listSongs");
    })
    .catch ((error) => {
      res.json({ message: "err" });
    });
});

// List all Songs
app.get("/listSongs", async (req, res) => {
  Songs.find(req.query)
    .then((music) => {
      res.render("index", { music: music });
    })
    .catch ((error)=> {
      res.json({ message: "err" });
    });
});

// List songs by Music Director
app.get("/songsByDirector/:director", async (req, res) => {
    Songs.find({ Music_director: req.params.director })
    .then((music)=>{
      res.render("index", { music: music });
    })
    .catch ((error) => {
      res.json({ message: "err" });
    });
});

// List songs by Music Director and Singer
app.get("/songsByDirectorAndSinger/:director/:singer", async (req, res) => {
  Songs.find({
      Music_director: req.params.director,
      singer: req.params.singer,
    })
    .then((music) => {
      res.render("/listSongs", { music: music });
    })
    .catch ((err) => {
      res.json({ message: "err" });
    });
});

// Delete song by ID
app.post("/deleteSong/:id", async (req, res) => {
  Songs.findByIdAndDelete(req.params.id)
  .then((music) => {
    res.redirect("/listSongs");
  })
  .catch ((error)=> {
    res.json({ message: "err" });
  });
});

// Add favorite song
app.post("/markFavorite/:id", async (req, res) => {
    const song = await Songs.findById(req.params.id);
    if (!song) {
      return res.status(404).send("Song not found");
    }
    song.favorite = true;
    await song.
    save()
    .then((item) => {
      res.redirect("/listFavoriteSongs");
    })
    .catch ((error) => {
      res.status(500).json({ message: "Error marking song as favorite" });
    });
});

app.get("/listFavoriteSongs", async (req, res) => {
  Songs.find({ favorite: true })
  .then((favoriteSongs) => {
    res.render("favoriteSongs", { favoriteSongs: favoriteSongs });
  })
  .catch ((error) => {
    res.status(500).json({ message: "Error retrieving favorite songs" });
  });
});


// List songs by Singer from specified Film
app.get("/songsBySingerAndFilm/:singer/:film", async (req, res) => {
  Songs.find({
    singer: req.params.singer,
    Film: req.params.film,
  })
  .then((music)=> {
    res.render("index", { music: music });
  })
  .catch ((error) => {
    res.json({ message: "err" });
  });
});

// Update song by ID to add Actor and Actress name
app.post("/updateSong/:id", async (req, res) => {
      const song = await Songs.findById(req.params.id);
      if (!song) {
          return res.status(404).send("Song not found");
      }
      song.actor = req.body.actor;
      song.actress = req.body.actress;
      await song.
      save()
      .then((item) => {
        res.redirect("/listSongs");
      }) 
      .catch ((error) => {
        res.status(500).json({ message: "Error updating song" });
      });
});

app.listen(3000, () => {
  console.log("Server is listening on port 3000");
});