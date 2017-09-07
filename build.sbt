
lazy val lsh = (project in file(".")).
  settings(Settings.settings: _*).
  settings(Settings.lshSettings: _*).
  settings(libraryDependencies ++=Dependencies.lshDependencies )