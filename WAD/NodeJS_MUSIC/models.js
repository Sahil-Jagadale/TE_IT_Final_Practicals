const mongoose = require("mongoose");

const SongsSchema = mongoose.Schema(
  {
    Songname: String,
    Film: String,
    Music_director: String,
    singer: String,
  },
  {
    timestamps: true,
  }
);

module.exports = mongoose.model("Songs", SongsSchema);