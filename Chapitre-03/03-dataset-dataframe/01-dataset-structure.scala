case class typePosteSynop(
                           Altitude:Long ,
                           ID:String,
                           Latitude:Double,
                           Longitude:Double,
                           Nom:String
                         )

val stationsDF00  =  spark.read.json("donnees/json/postes-synop.json").as[typePosteSynop]
stationsDF00.printSchema()

val stationsDF01 = stationsDF00.map(l =>
                  typePosteSynop(  l.Altitude ,
                                    l.ID,
                                    l.Latitude,
                                    l.Longitude,
                                    l.Nom.toLowerCase.split(' ').
                                      map(_.capitalize).mkString(" ").
                                      split('-').
                                      map(_.capitalize).
                                      mkString(" ") )
                                    ).as[typePosteSynop]
case class typeRPosteSynop(
                            Nom:String,
                            Altitude:Long ,
                            Longitude:Double
                         )
stationsDF01.map(l =>
                  typeRPosteSynop( l.Nom,
                                   l.Altitude ,
                                   l.Longitude)).
             as[typeRPosteSynop].
             filter( l => {l.Altitude > 400 && l.Longitude > 3 }).show()

stationsDF01.select( $"Nom", $"Altitude", $"Longitude").
        where("Altitude > 400").
        where("Longitude > 3").show()
