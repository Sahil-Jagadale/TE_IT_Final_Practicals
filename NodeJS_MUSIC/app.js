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
app.post("/addFavoriteSong", async (req, res) => {
    const song = new Songs(req.body);
    song.
      save()
      .then((item) => {
        res.redirect("/index");
      })
      .catch ((error) => {
        res.json({ message: "err" });
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
    song.Actor = req.body.Actor;
    song.Actress = req.body.Actress;
    await song
      .save()
      .then((music)=>{
        res.redirect("/index");
      })
      .catch ((error) => {
        res.json({ message: "err" });
      });
});

app.listen(3000, () => {
  console.log("Server is listening on port 3000");
});