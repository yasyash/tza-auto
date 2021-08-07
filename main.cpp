#include <QCoreApplication>
#include <QtCore/QDebug>
#include <QSqlQuery>
#include <QRegExp>
#include <QDateTime>
#include <QSqlRecord>
#include <QSqlError>
#include <QSqlField>
#include <QJsonObject>
#include <QJsonArray>
#include <QFile>
#include <math.h>

int main(int argc, char *argv[])
{
    QCoreApplication a(argc, argv);

    QDateTime _begin_time, _end_time, _curr_time;
    _begin_time =  QDateTime::fromString(argv[2],  "yyyy-MM-ddTHH:mm:ss");
    _curr_time = _begin_time;
    _end_time = QDateTime::fromString(argv[3],  "yyyy-MM-ddTHH:mm:ss");
    QMap<QDateTime, float> *time_frame = new  QMap<QDateTime, float>;

    while (_curr_time < _end_time)
    {

        _curr_time = _curr_time.addSecs(1200);
        if (_curr_time < _end_time)
        {
            time_frame->insert(_curr_time, 0.0f);
        } else {
            time_frame->insert(_end_time, 0.0f);
        }

    }




    QTextStream(stdout) << "Parameters " << argv[1] <<" - - - "<< _begin_time.toString() << " - - - " << _end_time.toString() << " - - - " << argv[4]<< endl;

    const QStringList template_chemical = (QStringList() <<"NO" << "NO2" << "NH3"<< "SO2" << "H2S"<< "O3"<< "CO"<< "CH2O"<< "PM1"<< "PM2.5"<< "PM10"<< "Пыль общая"<< "бензол"<< "толуол"<< "этилбензол"<< "м,п-ксилол"<< "о-ксилол"<< "хлорбензол"<< "стирол"<< "фенол");

    const QStringList template_weather = (QStringList() << "Темп. внешняя" << "Направление ветра" << "Скорость ветра" << "Влажность внеш."  );


    QMap<QString, int> *chemical_classes = new  QMap<QString, int>;

    chemical_classes->insert("NO",3);
    chemical_classes->insert("NO2", 3);
    chemical_classes->insert("NH3", 4);
    chemical_classes->insert("SO2", 3);
    chemical_classes->insert("H2S", 3);
    chemical_classes->insert("O3", 1);
    chemical_classes->insert("CO", 4);
    chemical_classes->insert("CH2O", 2);
    chemical_classes->insert("PM1", 3);
    chemical_classes->insert("PM2.5", 3);
    chemical_classes->insert("PM10", 3);
    chemical_classes->insert("Пыль общая", 3);
    chemical_classes->insert("бензол", 2);
    chemical_classes->insert("толуол", 3);
    chemical_classes->insert("этилбензол", 4);
    chemical_classes->insert("м,п-ксилол", 3);
    chemical_classes->insert("о-ксилол", 3);
    chemical_classes->insert("хлорбензол", 3);
    chemical_classes->insert("стирол", 3);
    chemical_classes->insert("фенол", 2);


    QStringListIterator template_chemical_iterator(template_chemical);
    QStringListIterator template_weather_iterator(template_weather);

    QString _chemical;

    QMap<QString, QVariant> *jsonobj, * footer;
    QMap<QString, QMap<QString, QVariant> *> *chemical_by_day_chain;

    jsonobj= new  QMap<QString, QVariant>;
    footer = new QMap<QString, QVariant>;

    chemical_by_day_chain = new  QMap<QString, QMap<QString, QVariant>*>;

    while (template_chemical_iterator.hasNext())
    {


        _chemical = template_chemical_iterator.next();

        //_chemical_chain{{"срака овечья", 1}};


        //chemical_chain[_chemical1.trimmed()] = -1.0;
        jsonobj->insert(_chemical, -1.0);
        //jsonobj->insert(QString(_chemical).append("_range"), false);
        //jsonobj->insert(QString(_chemical).append("_empty"), false);

        footer->insert(QString(_chemical).append("_exceed1"), 0);
        footer->insert(QString(_chemical).append("_exceed5"), 0);
        footer->insert(QString(_chemical).append("_exceed10"), 0);
        footer->insert(QString(_chemical).append("_average"), -1.0);
        footer->insert(QString(_chemical).append("_count"), 0);
        footer->insert(QString(_chemical).append("_max"), 0.0);
        footer->insert(QString(_chemical).append("_time_max"), 0);
        footer->insert(QString(_chemical).append("_min"), -1.0);
        footer->insert(QString(_chemical).append("_time_min"), 0);
        footer->insert(QString(_chemical).append("_max_consentration"), -1.0);
        footer->insert(QString(_chemical).append("_time_max_consentration"), 0);
        footer->insert(QString(_chemical).append("_sindex"), -1.0);
        footer->insert(QString(_chemical).append("_greatest_repeatably"), -1.0);
        footer->insert(QString(_chemical).append("_sigma"), -1.0);


        //  chemical_chain.append(QJsonValue( {QString(_chemical).append("_range") , false}));
        // chemical_chain.append(QJsonValue( {QString(_chemical).append("_empty") , false}));

    }

    _curr_time = _begin_time;
    //_curr_time =_curr_time.addSecs(1200);
    while (_curr_time < _end_time)
    {
        QMap<QString, QVariant> *jsonobj_cpy =  new QMap<QString, QVariant>(  *jsonobj);
        //memcpy(jsonobj_cpy, jsonobj, sizeof (*jsonobj));



        _curr_time = _curr_time.addSecs(1140);

        if (_curr_time < _end_time)
        {
            //  chemical_daily_chain[_curr_time.toString("yyyy-MM-ddTHH:mm:ss")] = chemical_chain;
            chemical_by_day_chain->insert(_curr_time.toString("yyyy-MM-dd HH:mm"), jsonobj_cpy );
            //{"NO" , "NO2" , "NH3", "SO2" , "H2S", "O3", "CO", "CH2O", "PM1", "PM2.5", "PM10", "Пыль общая", "бензол", "толуол", "этилбензол", "м,п-ксилол", "о-ксилол", "хлорбензол", "стирол", "фенол" });

        } else {
            chemical_by_day_chain->insert(_end_time.toString("yyyy-MM-dd HH:mm"), jsonobj_cpy );

        }
        _curr_time = _curr_time.addSecs(60);


    }

    //QVariant ooo = chemical_by_day_chain->value("01")->value("NO");


    QSqlDatabase * m_conn = new QSqlDatabase();
    *m_conn = QSqlDatabase::addDatabase("QPSQL");
    m_conn->setHostName("localhost");
    m_conn->setDatabaseName("weather");
    m_conn->setUserName("weather");
    m_conn->setPassword("31415");

    bool status = m_conn->open();
    if (!status)
    {
        //releaseModbus();

        QTextStream(stdout) << ( QString("Connection error: " + m_conn->lastError().text()).toLatin1().constData()) <<   " \n\r";
        return -1;

    }

    //range threshoulds load

    QMap<QString, float> *chemical_ranges = new  QMap<QString, float>;

    QSqlQuery *query_equipments= new QSqlQuery ("select * from equipments where is_present = 'true' and idd = '"+ QString(argv[1]) +"'", *m_conn);

    QSqlRecord rec;

    query_equipments->first();

    for (int i = 0; i < query_equipments->size(); i++ )

    {
        rec = query_equipments->record();
        if (rec.field("max_day_consentration").value().toFloat() >0)
            chemical_ranges->insert(rec.field("typemeasure").value().toString(),rec.field("max_day_consentration").value().toFloat());

        query_equipments->next();


    }
    query_equipments->finish();


    // macs table load
    QSqlQuery *query_macs= new QSqlQuery ("select * from macs", *m_conn);

    QMap<QString, float>  *m_macs =  new QMap<QString, float>;

    query_macs->first();

    for (int i = 0; i < query_macs->size(); i++ )
    {

        rec = query_macs->record();

        m_macs->insert(rec.field("chemical").value().toString(),rec.field("max_m").value().toFloat());
        query_macs->next();

    }

    query_macs->finish();



    //measure load
    QDateTime _tmp_dtime, min_dtime, max_dtime, min_macs_dtime, max_macs_dtime, day_current;
    QMap<QDateTime, float>::iterator _dtime_iterator;
    QSqlQuery *query;
    float _measure, _tmp_measure, min_measure, max_measure, min_macs, max_macs, total_sum, day_sum, standard_index, greatest_repeatably, sigma, sum_pow2,range;
    int _cnt, total_cnt, frames_cnt, day_counter, day_number;
    int local_counter_macs1, local_counter_macs5, local_counter_macs10;
    bool outranged = false;

    template_chemical_iterator.toFront();

    while (template_chemical_iterator.hasNext())
    {


        QString _chemical = template_chemical_iterator.next();
        query = new QSqlQuery ("select * from sensors_data where idd='" + QString(argv[1]) +"' and date_time >= '"+ QString(argv[2]) +"' and date_time < '"+
                QString(argv[3])+"' and typemeasure = '" + _chemical +"'order by date_time asc", *m_conn);
        query->first();

        total_sum = 0.0f;
        total_cnt = 0;
        frames_cnt = 0;
        day_counter = 0;
        day_sum = 0.0f;
        day_number  = 0;

        local_counter_macs1 = 0;
        local_counter_macs5 = 0;
        local_counter_macs10 = 0;

        min_macs = 1000000.0f;
        max_macs = -1.0f;
        min_measure = 1000000.0f;
        max_measure = -1.0f;

        range = chemical_ranges->value(_chemical);

        _dtime_iterator = time_frame->begin();
        day_current = _dtime_iterator.key();

        while (_dtime_iterator != time_frame->end()) {
            _measure = 0.0f;
            _cnt = 0;


            rec = query->record();

            _tmp_measure = (rec.field("measure").value().toFloat());
            _tmp_dtime = (rec.field("date_time").value().toDateTime());

            while ((_tmp_dtime < _dtime_iterator.key()) &&(_tmp_dtime.isValid())){

                _measure += _tmp_measure;
                _cnt++;
                day_sum += _tmp_measure;
                day_counter++;
                if(_tmp_measure > range)
                    outranged = true;
                //if (_tmp_measure > max_measure )
                //{
                //      max_measure = _tmp_measure;
                //    max_dtime =  _tmp_dtime;
                // }

                // if (_tmp_measure < min_measure )
                // {
                //    min_measure = _tmp_measure;
                //     min_dtime =  _tmp_dtime;
                // }
                if (!query->next())
                    break;
                rec = query->record();
                _tmp_measure = (rec.field("measure").value().toFloat());
                _tmp_dtime = (rec.field("date_time").value().toDateTime());


            }
            if (_cnt)
            {
                QDateTime _date_time_key;
                time_frame->insert(_dtime_iterator.key(), _measure/_cnt);
                if (_dtime_iterator.key() < _end_time){
                    _date_time_key = QDateTime(_dtime_iterator.key()).addSecs(-59);
                }
                else {
                    _date_time_key = _dtime_iterator.key();
                }
                chemical_by_day_chain->value(_date_time_key.toString("yyyy-MM-dd HH:mm"))->insert(_chemical, _measure/_cnt);

                //macs counter increment
                if (_dtime_iterator.value()/10 > m_macs->value(_chemical))
                { local_counter_macs10++;
                } else {
                    if (_dtime_iterator.value()/5 > m_macs->value(_chemical))
                    {
                        local_counter_macs5 ++;
                    } else {
                        if (_dtime_iterator.value() > m_macs->value(_chemical))
                            local_counter_macs1++;
                    }
                }

                if ((_cnt < 9) ) // if measure less than 1/2 quantity of 20-minutes inrervals per frame
                {
                    chemical_by_day_chain->value(_date_time_key.toString("yyyy-MM-dd HH:mm"))->insert(QString(_chemical).append("_empty"), true);

                }

                if ((_cnt > 8))
                {
                    chemical_by_day_chain->value(_date_time_key.toString("yyyy-MM-dd HH:mm"))->insert(QString(_chemical).append("_empty"), false);

                }

                if (outranged)
                    chemical_by_day_chain->value(_date_time_key.toString("yyyy-MM-dd HH:mm"))->insert(QString(_chemical).append("_range"), true);

                if (!outranged )
                    chemical_by_day_chain->value(_date_time_key.toString("yyyy-MM-dd HH:mm"))->insert(QString(_chemical).append("_range"), false);

                if (local_counter_macs1)
                    chemical_by_day_chain->value(_date_time_key.toString("yyyy-MM-dd HH:mm"))->insert(QString(_chemical).append("_macs"), 1);

                if (local_counter_macs5)
                    chemical_by_day_chain->value(_date_time_key.toString("yyyy-MM-dd HH:mm"))->insert(QString(_chemical).append("_macs"), 5);

                if(local_counter_macs10)
                    chemical_by_day_chain->value(_date_time_key.toString("yyyy-MM-dd HH:mm"))->insert(QString(_chemical).append("_macs"), 10);


                local_counter_macs1 = 0;
                local_counter_macs5 = 0;
                local_counter_macs10 = 0;
                outranged = false;
                           }


            _dtime_iterator++;

        }

        query->finish();
        query->~QSqlQuery();

    }

    //weather calculation
    template_weather_iterator.toFront();

    while (template_weather_iterator.hasNext())
    {


        QString _chemical = template_weather_iterator.next();
        query = new QSqlQuery ("select * from sensors_data where idd='" + QString(argv[1]) +"' and date_time >= '"+ QString(argv[2]) +"' and date_time < '"+
                QString(argv[3])+"' and typemeasure = '" + _chemical +"'order by date_time asc", *m_conn);
        query->first();

        _measure = 0.0f;
        _cnt = 0;

        _dtime_iterator = time_frame->begin();
        day_current = _dtime_iterator.key();

        while (_dtime_iterator != time_frame->end()) {


            rec = query->record();

            _tmp_measure = (rec.field("measure").value().toFloat());
            _tmp_dtime = (rec.field("date_time").value().toDateTime());

            while ((_tmp_dtime < _dtime_iterator.key()) &&(_tmp_dtime.isValid())){

                _measure += _tmp_measure;
                _cnt++;


                if (!query->next())
                    break;
                rec = query->record();
                _tmp_measure = (rec.field("measure").value().toFloat());
                _tmp_dtime = (rec.field("date_time").value().toDateTime());


            }



            if (_cnt)
            {
                QDateTime _date_time_key;
                float _day_measure = (_measure / _cnt);
                if (_dtime_iterator.key() < _end_time){
                    _date_time_key = QDateTime(_dtime_iterator.key()).addSecs(-59);
                }
                else {
                    _date_time_key = _dtime_iterator.key();

                }
                chemical_by_day_chain->value(_date_time_key.toString("yyyy-MM-dd HH:mm"))->insert(_chemical, _day_measure);
                _measure = 0.0f;
                _cnt = 0;
            }
            _dtime_iterator++;


        }
    }

    //file creation

    QFile file("./api/tza.csv");
    if (!file.open(QIODevice::WriteOnly | QIODevice::Text))
        return -1;

    QTextStream out(&file);
    QMap<QString, QMap<QString, QVariant> *>::iterator _i;


    out << "date;";

    template_weather_iterator.toFront();

    while (template_weather_iterator.hasNext())
    {
        QString _chemical = template_weather_iterator.next();

        out << _chemical << ";";

    }

    template_chemical_iterator.toFront();

    while (template_chemical_iterator.hasNext())
    {
        QString _chemical = template_chemical_iterator.next();

        out << _chemical << ";";

    }
    out << "\n";

    for ( _i = chemical_by_day_chain->begin(); _i != chemical_by_day_chain->end();++_i)

    {
        template_weather_iterator.toFront();
        //out << QString(_i.key()).append("_date") << ";";
        out <<_i.key() <<";";

        //weather out
        while (template_weather_iterator.hasNext())
        {
            QString _chemical = template_weather_iterator.next();

            QList< QVariant> _jsonobj_cpy =  _i.value()->values(_chemical);
            if (_jsonobj_cpy.size() == 0)
            {
                out <<  "-;";
            } else {
                out << _jsonobj_cpy.first().toString() << ";";

            }

        }
        // measure out
        template_chemical_iterator.toFront();

        while (template_chemical_iterator.hasNext())
        {
            QString _chemical = template_chemical_iterator.next();
            QList< QVariant> _jsonobj_cpy =  _i.value()->values(_chemical);
            if (_jsonobj_cpy.first().toInt() == -1)
            {
                out <<  "-;";
            } else {
                out << _jsonobj_cpy.first().toString() << ";";

            }

        }
        out << "\n";

        //errors out

        template_chemical_iterator.toFront();
        out << _i.key()<< "_empty;;;;;";


        //measures empty out

        while (template_chemical_iterator.hasNext())
        {
            QString _chemical = template_chemical_iterator.next();
            QList< QVariant> _jsonobj_cpy =  _i.value()->values(QString(_chemical).append("_empty"));
            if (_jsonobj_cpy.size() > 0) {
                if (!_jsonobj_cpy.first().toBool())
                {
                    out <<  "false;";
                } else {
                    out <<  "true;";

                }
            } else {
                out << "-;";
            }

        }
        out << "\n";

        //outranges out
        template_chemical_iterator.toFront();
        out << _i.key()<< "_outrange;;;;;";

        while (template_chemical_iterator.hasNext())
        {
            QString _chemical = template_chemical_iterator.next();
            QList< QVariant> _jsonobj_cpy =  _i.value()->values(QString(_chemical).append("_range"));
            if (_jsonobj_cpy.size() > 0) {
                if (!_jsonobj_cpy.first().toBool())
                {
                    out <<  "false;";
                } else {
                    out <<  "true;";

                }
            } else {
                out << "-;";
            }


        }
        out << "\n";

        //macs css out
        template_chemical_iterator.toFront();
        out << _i.key()<< "_macs;;;;;";

        while (template_chemical_iterator.hasNext())
        {
            QString _chemical = template_chemical_iterator.next();
            QList< QVariant> _jsonobj_cpy =  _i.value()->values(QString(_chemical).append("_macs"));

            if (_jsonobj_cpy.size() > 0) {
                out << _jsonobj_cpy.first().toString() << ";";
            } else {
                out << "-;";
            }


        }
        out << "\n";

    }


    file.close();

    return 0;
}
