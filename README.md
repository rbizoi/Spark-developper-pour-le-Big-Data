

# Spark développer pour le Big Data
<img src="https://github.com/rbizoi/Spark-developper-pour-le-Big-Data/blob/master/images/presentation.png" width="512">



## 01-L'écosystème Big Data Analytique

Une architecture de type Big Data Analytique doit permettre d'ingérer, consolider, traiter et analyser à grande vitesse en flux continu les données. Le traitement de flux doit être rapide, évolutif, tolérant aux pannes et de bout en bout, sans que l'utilisateur ait à se soucier du flux.<br>
<img src="https://github.com/rbizoi/Spark-developper-pour-le-Big-Data/blob/master/Chapitre-01/images/M01.06.png" width="512">

La structure de la machine virtuelle.<br>

<img src="https://github.com/rbizoi/Spark-developper-pour-le-Big-Data/blob/master/Chapitre-01/images/M01.07.png" width="512">

<table>
<tr><th align="left">1.     </th><th align="left">Le déluge de données                             </th><th align="left"></th><th align="left"></th></tr>
<tr><th align="left">2.     </th><th align="left">Les systèmes de calcul distribué                 </th><th align="left"></th><th align="left"></th></tr>
<tr><th align="left">3.     </th><th align="left">Le système de fichiers distribué Hadoop (HDFS)   </th><th align="left"></th><th align="left"></th></tr>
<tr><th align="left">4.     </th><th align="left">Le système de traitements MapReduce              </th><th align="left"></th><th align="left"></th></tr>
<tr><th align="left">5.     </th><th align="left">Les extensions MapReduce                         </th><th align="left"></th><th align="left"></th></tr>
<tr><th align="left">6.     </th><th align="left">Le Big Data Analytique                           </th><th align="left"></th><th align="left"></th></tr>
<tr><th align="left">6.1.   </th><th align="left"></th><th align="left">La découverte                                    </th><th align="left"></th></tr>
<tr><th align="left">6.2.   </th><th align="left"></th><th align="left">La préparation des données                       </th><th align="left"></th></tr>
<tr><th align="left">6.3.   </th><th align="left"></th><th align="left">La conception du modèle                          </th><th align="left"></th></tr>
<tr><th align="left">6.4.   </th><th align="left"></th><th align="left">La construction du modèle                        </th><th align="left"></th></tr>
<tr><th align="left">6.5.   </th><th align="left"></th><th align="left">Les résultats                                    </th><th align="left"></th></tr>
<tr><th align="left">6.6.   </th><th align="left"></th><th align="left">La mise en production                            </th><th align="left"></th></tr>
<tr><th align="left">7.     </th><th align="left"></th><th align="left"></th><th align="left">L’analyse de données en flux continu             </th></tr>
<tr><th align="left">8.     </th><th align="left"></th><th align="left"></th><th align="left">L’installation                                   </th></tr>
<tr><th align="left">9.     </th><th align="left"></th><th align="left"></th><th align="left">Les produits nécessaires                         </th></tr>
<tr><th align="left">10.    </th><th align="left"></th><th align="left"></th><th align="left">Installer les prérequis                          </th></tr>
<tr><th align="left">10.1.  </th><th align="left"></th><th align="left"></th><th align="left">La configuration du système d'exploitation       </th></tr>
<tr><th align="left">10.2.  </th><th align="left"></th><th align="left"></th><th align="left">L'installation de l'environnement Java           </th></tr>
<tr><th align="left">10.3.  </th><th align="left"></th><th align="left"></th><th align="left">L'installation du langage Python                 </th></tr>
<tr><th align="left">10.4.  </th><th align="left"></th><th align="left"></th><th align="left">L'installation du langage R                      </th></tr>
<tr><th align="left">10.5.  </th><th align="left"></th><th align="left"></th><th align="left">La création des utilisateurs                     </th></tr>
<tr><th align="left">10.6.  </th><th align="left"></th><th align="left"></th><th align="left">La configuration automatique des prérequis       </th></tr>
<tr><th align="left">11.    </th><th align="left"></th><th align="left"></th><th align="left">Installer Apache Hadoop                          </th></tr>
<tr><th align="left">12.    </th><th align="left"></th><th align="left"></th><th align="left">Installer Apache Spark                           </th></tr>
<tr><th align="left">12.1.  </th><th align="left"></th><th align="left"></th><th align="left">L'installation des fichiers                      </th></tr>
<tr><th align="left">12.2.  </th><th align="left"></th><th align="left"></th><th align="left">La configuration de l'environnement              </th></tr>
<tr><th align="left">12.3.  </th><th align="left"></th><th align="left"></th><th align="left">Les environnements de commandes                  </th></tr>
<tr><th align="left">12.3.1.</th><th align="left"></th><th align="left"></th><th align="left">L'environnement de commande Scala                </th></tr>
<tr><th align="left">12.3.2.</th><th align="left"></th><th align="left"></th><th align="left">L'environnement de commande Python               </th></tr>
<tr><th align="left">12.3.3.</th><th align="left"></th><th align="left"></th><th align="left">L'environnement de commande R                    </th></tr>
<tr><th align="left">12.4.  </th><th align="left"></th><th align="left"></th><th align="left">L'installation et intégration avec Apache Hadoop </th></tr>
<tr><th align="left">13.    </th><th align="left"></th><th align="left"></th><th align="left">Installer Apache Hive                            </th></tr>
<tr><th align="left">14.    </th><th align="left"></th><th align="left"></th><th align="left">Installer Apache Zookeeper                       </th></tr>
<tr><th align="left">15.    </th><th align="left"></th><th align="left"></th><th align="left">Installer Apache Kafka                           </th></tr>
<tr><th align="left">16.    </th><th align="left"></th><th align="left"></th><th align="left">Installer Apache Zeppelin                        </th></tr>
<tr><th align="left">17.    </th><th align="left"></th><th align="left"></th><th align="left">Configurer le démarrage et l'arrêt du cluster    </th></tr>
<tr><th align="left">17.1.  </th><th align="left"></th><th align="left"></th><th align="left">Apache Hadoop                                    </th></tr>
<tr><th align="left">17.2.  </th><th align="left"></th><th align="left"></th><th align="left">Jupyter Notebook                                 </th></tr>
<tr><th align="left">17.3.  </th><th align="left"></th><th align="left"></th><th align="left">Apache Zeppelin                                  </th></tr>
</table>

## 02-L’architecture
<table>
<tr><th align="left">1.      </th><th align="left">Qu'est-ce que Spark ?                                 </th></tr>
<tr><th align="left">1.1.    </th><th align="left">Spark SQL                                             </th></tr>
<tr><th align="left">1.2.    </th><th align="left">Spark Streaming                                       </th></tr>
<tr><th align="left">1.3.    </th><th align="left">Machine Learning                                      </th></tr>
<tr><th align="left">1.4.    </th><th align="left">GraphX                                                </th></tr>
<tr><th align="left">1.5.    </th><th align="left">Le Big Data Analytique Unifié                         </th></tr>
<tr><th align="left">2.      </th><th align="left">L'architecture distribuée                             </th></tr>
<tr><th align="left">2.1.    </th><th align="left">Spark Driver                                          </th></tr>
<tr><th align="left">2.2.    </th><th align="left">Spark Session                                         </th></tr>
<tr><th align="left">2.3.    </th><th align="left">Spark Manager                                         </th></tr>
<tr><th align="left">2.4.    </th><th align="left">Spark Worker                                          </th></tr>
<tr><th align="left">2.5.    </th><th align="left">Spark Executor                                        </th></tr>
<tr><th align="left">2.6.    </th><th align="left">RDD et DAG                                            </th></tr>
<tr><th align="left">2.6.1.  </th><th align="left">RDD                                                   </th></tr>
<tr><th align="left">2.6.2.  </th><th align="left">Transformations                                       </th></tr>
<tr><th align="left">2.6.3.  </th><th align="left">Actions                                               </th></tr>
<tr><th align="left">2.6.4.  </th><th align="left">DAG                                                   </th></tr>
<tr><th align="left">2.6.5.  </th><th align="left">La résolution des pannes                              </th></tr>
<tr><th align="left">3.      </th><th align="left">L'architecture d'une application                      </th></tr>
<tr><th align="left">3.1.    </th><th align="left">Spark Job                                             </th></tr>
<tr><th align="left">3.2.    </th><th align="left">Spark Stage                                           </th></tr>
<tr><th align="left">3.3.    </th><th align="left">Spark Task                                            </th></tr>
<tr><th align="left">4.      </th><th align="left">Transformations, Actions et DAG avec les DataFrames   </th></tr>
</table>

## 03-La structure et les types de données

## 04-Les traitements et le flux de données

## 05-L’exploration, la préparation et la visualisation des données

## 06-Le « Machine Learning »

## 07-Le « Deep Learning »

## 08-La mise en production
