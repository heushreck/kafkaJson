import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException
import java.util.*


fun main(args: Array<String>) {
    if (args.isNullOrEmpty()) {
        println("Usage: java -jar producer.jar [server-address]")
        return
    }

    val properties = Properties()
    properties["bootstrap.servers"] = args[0]
    properties["key.serializer"] = StringSerializer::class.java
    properties["value.serializer"] = ByteArraySerializer::class.java
    val producer = Producer<ByteArray>(properties)

    sampleRun(producer)
    Run(producer, "events.json")
    producer.finalize()
}

fun sampleRun(sampleProducer: Producer<ByteArray>){
    val json = "{\"utc_offset\":0,\"venue\":{\"country\":\"gb\",\"city\":\"London\",\"address_1\":\"Bramley Road\",\"name\":\"Oakwood Tube, Piccadilly Line, North London\",\"lon\":-0.13532,\"lat\":51.64735},\"rsvp_limit\":17,\"venue_visibility\":\"public\",\"visibility\":\"public\",\"fee\":{\"description\":\"per person\",\"amount\":14.5,\"currency\":\"GBP\"},\"maybe_rsvp_count\":0,\"description\":\"<p><span><strong>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; * * * SUNDAY * * *<\\/strong><\\/span><\\/p>\\n<p><span><strong>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; FREE December Winter Walk<\\/strong><\\/span><\\/p>\\n<p><span><strong>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;London Loop Section 17<\\/strong><\\/span><\\/p>\\n<p><img src=\\\"http:\\/\\/photos4.meetupstatic.com\\/photos\\/event\\/a\\/6\\/c\\/b\\/600_431682699.jpeg\\\"><\\/p>\\n<p><span><strong>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; Cockfosters to Enfield Lock<\\/strong><\\/span><\\/p>\\n<p><b>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;Ending with 2-course Festive Lunch at Inn On the Park<\\/b><\\/p>\\n<p><a href=\\\"https:\\/\\/www.tabletable.co.uk\\/pub-restaurant-menus\\/Greater-London\\/Inn-On-The-Park-Enfield\\/festive_menu.htm\\\">https:\\/\\/www.tabletable.co.uk\\/pub-restaurant-menus\\/Greater-London\\/Inn-On-The-Park-Enfield\\/festive_menu.htm<\\/a><br> <\\/p>\\n<p><b>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; Price includes: Festive lunch and PayPal fee.<\\/b><br><\\/p>\\n<p><b>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;* * * * *&nbsp;<\\/b><\\/p>\\n<p><strong><span>The&nbsp;<b>London Outer Orbital Path<\\/b>&nbsp;— more usually the&nbsp;<b>\\\"London LOOP\\\"<\\/b>&nbsp;— is a 240-kilometre (150&nbsp;mi) signed walk along&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Rights_of_way_in_England_and_Wales\\\">public footpaths<\\/a>, and through parks, woods and fields around the edge of&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Outer_London\\\">Outer London<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/England\\\">England<\\/a>, described as \\\"the&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/M25_motorway\\\">M25<\\/a>&nbsp;for walkers\\\". The walk begins at&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Erith\\\">Erith<\\/a>&nbsp;on the south bank of the&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/River_Thames\\\">River Thames<\\/a>&nbsp;and passes clockwise through&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Crayford\\\">Crayford<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Petts_Wood\\\">Petts Wood<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Coulsdon\\\">Coulsdon<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Banstead\\\">Banstead<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Ewell\\\">Ewell<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Kingston_upon_Thames\\\">Kingston upon Thames<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Uxbridge\\\">Uxbridge<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Elstree\\\">Elstree<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Cockfosters\\\">Cockfosters<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Chingford\\\">Chingford<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Chigwell\\\">Chigwell<\\/a>,&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Grange_Hill\\\">Grange Hill<\\/a>&nbsp;and&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Upminster_Bridge\\\">Upminster Bridge<\\/a>before ending at&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Purfleet\\\">Purfleet<\\/a>, almost directly across the&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/River_Thames\\\">Thames<\\/a>&nbsp;from its starting point. Between these settlements the route passes through&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Buffer_zone\\\">green buffers<\\/a>&nbsp;and some of the highest points in&nbsp;<a href=\\\"http:\\/\\/en.wikipedia.org\\/wiki\\/Greater_London\\\">Greater London<\\/a>.<br> <\\/span><\\/strong><\\/p>\\n<p><img src=\\\"http:\\/\\/photos3.meetupstatic.com\\/photos\\/event\\/c\\/8\\/3\\/4\\/600_312411252.jpeg\\\"><\\/p>\\n<p>This extended (<b>start at Oakwood tube<\\/b>) stretch of the London Loop takes in the landscaped grounds of Trent Park, the hills and woods of Enfield Chase, through places known as Botany Bay and Cuckold’s Hill to the less-than-sylvan pavements of Enfield itself. Click through the gallery for a photo tour.<\\/p>\\n<p><strong><span>London Country retreat<\\/span><\\/strong><br> Join us for a FREE SUNDAY Winter Walk across 413 acres of rolling meadows, enchanting brooks, exquisite lakes, ancient woodland, and imposing historical sites. Trent park forms part of the London Loop Section 17, and being a country park it provides a natural, rural and tranquil atmosphere right on the outskirts of London on the borders of Enfield.<\\/p>\\n<p>The walk will limited in member numbers lasting approximately 4.5hrs in total and start at 10am from Cockfosters tube (Piccadilly Line). We will enter Trent Park via the main Cockfosters Road enterance and walk through the ancient woodland and rolling meadows.<\\/p>\\n<p><strong><img src=\\\"http:\\/\\/photos3.meetupstatic.com\\/photos\\/event\\/2\\/7\\/1\\/2\\/event_119170002.jpeg\\\" width=\\\"246\\\" height=\\\"148\\\"><\\/strong><\\/p>\\n<p>We will visit The Mansion, which was Middlesex University, the Solitary Obelisk and Camlet Moat which are just some of the remarkable historical sites and an ornate Water Garden.<\\/p>\\n<p><strong><img src=\\\"http:\\/\\/photos1.meetupstatic.com\\/photos\\/event\\/e\\/a\\/1\\/c\\/event_119279932.jpeg\\\" width=\\\"235\\\" height=\\\"197\\\"><\\/strong><\\/p>\\n<p>Camlet Moat is a scheduled Ancient Monument and is protected under the 1979 Ancient Monument and Archaeological Areas. Nothing is known of the origins of Camlet Moat. The first probable occupant was Richard Pounz, Keeper of the Chase in the 1320s. In 1429 the lodge was demolished and the materials sold to help pay for repairs to Hertford Castle.<\\/p>\\n<p><strong><img src=\\\"http:\\/\\/photos4.meetupstatic.com\\/photos\\/event\\/2\\/6\\/1\\/8\\/event_119169752.jpeg\\\" width=\\\"232\\\" height=\\\"173\\\"><\\/strong><\\/p> \\n<p><strong>Level:&nbsp;<\\/strong><\\/p>\\n<p><span>Easy to Moderate.<\\/span><\\/p>\\n<p><strong>Time\\/Distance:<\\/strong>&nbsp;5hrs - 10miles (16km)<br> <br> <strong>Meeting point:<\\/strong> Oakwood Tube (Piccadilly line) station<br> <br> <strong>Meeting time:<\\/strong> 9.45am (Walk starts at 10am). Sunday 11th December 2016.<\\/p> \\n<p><b>Price includes: Festive lunch and PayPal fee.<\\/b><\\/p>\\n<p><b>Ending with 2-course Festive Lunch at Inn On The Park. See:&nbsp;<\\/b><a href=\\\"https:\\/\\/www.tabletable.co.uk\\/pub-restaurant-menus\\/Greater-London\\/Inn-On-The-Park-Enfield\\/festive_menu.html\\\">https:\\/\\/www.tabletable.co.uk\\/pub-restaurant-menus\\/Greater-London\\/Inn-On-The-Park-Enfield\\/festive_menu.html<\\/a><\\/p>\\n<p><b>The 2-course festive lunch includes PayPal fee= £14.50.<\\/b><\\/p>\\n<p><b>&nbsp;After lunch w<\\/b><b>e will take the bus from&nbsp;Enfield&nbsp;and return to Oakwood tube (Piccadilly line).<\\/b><\\/p>\\n<p><strong><span>NOTE:<\\/span> Please remember to change your RSVP if you are unable to attend the festive dinner (compulsory) or no longer able to make the event.<\\/strong> This is a FREE event so please ensure you update you status allowing plenty of time for others to RSVP, as member numbers are limited for the FREE Winter Walk. Members&nbsp;<b>MUST<\\/b> agree to a 2-course festive lunch.&nbsp;<\\/p>\\n<p><strong>Things to bring:<\\/strong><br> Water<br> Snacks<br> Waterproof clothing (weather dependant)<br> Walking shoes\\/boots<br> Money (cash for drinks\\/Sunday Roast)<br> Camera (optional)<\\/p>\\n<p><strong><span>IMPORTANT NOTE:<\\/span><\\/strong><br> By taking part in this meetup you agree to the following disclaimer:<br> <\\/p>\\n<p><span>We consider safety to be our number one priority and while we never take unnecessary risks, we do recognise that any outdoor activity involves a danger of personal injury or death. Remember that you should be aware of and accept these risks as you are responsible for your own safety and you should not undertake anything beyond your abilities. It is also your responsibility to be correctly equipped for the weather and activity you have chosen to participate in.<\\/span><\\/p>\\n<p><br> <span>We recommend that everyone participating in outdoor activities obtain appropriate insurance. The BMC offer excellent policies for all outdoor enthusiasts at very competitive rates:<a href=\\\"https:\\/\\/www.thebmc.co.uk\\/modules\\/insurance\\/Landing.aspx\\\">https:\\/\\/www.thebmc.co.uk\\/modules\\/insurance\\/Landing.aspx<\\/a><\\/span><\\/p>\",\"mtime\":1479120854391,\"event_url\":\"https:\\/\\/www.meetup.com\\/NBH-NaturalBornHikers\\/events\\/235569661\\/\",\"yes_rsvp_count\":1,\"payment_required\":\"1\",\"name\":\"December Winter Walk - London Loop Section 17: Cockfosters to Enfield Lock.\",\"id\":\"235569661\",\"time\":1481449500000,\"group\":{\"join_mode\":\"approval\",\"country\":\"gb\",\"city\":\"London\",\"name\":\"NBH-Natural Born Hikers - London International Hikers.\",\"group_lon\":-0.13,\"id\":1732609,\"state\":\"17\",\"urlname\":\"NBH-NaturalBornHikers\",\"category\":{\"name\":\"outdoors\\/adventure\",\"id\":23,\"shortname\":\"outdoors-adventure\"},\"group_photo\":{\"highres_link\":\"http:\\/\\/photos3.meetupstatic.com\\/photos\\/event\\/b\\/6\\/5\\/a\\/highres_398986682.jpeg\",\"photo_link\":\"http:\\/\\/photos3.meetupstatic.com\\/photos\\/event\\/b\\/6\\/5\\/a\\/600_398986682.jpeg\",\"photo_id\":398986682,\"thumb_link\":\"http:\\/\\/photos3.meetupstatic.com\\/photos\\/event\\/b\\/6\\/5\\/a\\/thumb_398986682.jpeg\"},\"group_lat\":51.52},\"status\":\"upcoming\"}\n"

    sampleProducer.produce("event_test", "event: ", json.toByteArray())
}

fun Run(producer: Producer<ByteArray>, filepath: String){
    val reader: BufferedReader
    try {
        reader = BufferedReader(FileReader(filepath))
        var line = reader.readLine()
        while (line != null) {
            producer.produce("event_test", "event: ", line.toByteArray())
            line = reader.readLine()
        }
        reader.close()
    } catch (e: IOException) {
        e.printStackTrace()
    }
}

class Producer<X>(properties: Properties) {

    var properties: Properties = properties
    var kafkaProducer: KafkaProducer<String, X> = KafkaProducer<String, X>(properties)


    fun produce(topicName:String, key:String , value: X){
        val producerRecord = ProducerRecord(topicName, key, value)
        kafkaProducer.send(producerRecord)
    }

    fun finalize() {
        kafkaProducer.close()
    }
}
